dofile("table_show.lua")
dofile("urlcode.lua")
dofile("strict.lua")
local urlparse = require("socket.url")
local luasocket = require("socket") -- Used to get sub-second time
local http = require("socket.http")
JSON = assert(loadfile "JSON.lua")()

local start_urls = JSON:decode(os.getenv("start_urls"))
local items_table = JSON:decode(os.getenv("item_names_table"))
local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local set_item_yet = false

local discovered_items = {}
local last_main_site_time = 0
local current_item_type = nil
local current_item_value = nil
local next_start_url_index = 1

local PAGE_SIZE = 30
local DATE_SLICE_TARGET_SECONDS = 1200

local last_id_result = nil
local previously_queued_date = nil


io.stdout:setvbuf("no") -- So prints are not buffered - http://lua.2524044.n2.nabble.com/print-stdout-and-flush-td6406981.html

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local do_debug = false
print_debug = function(a)
  if do_debug then
    print(a)
  end
end
print_debug("This grab script is running in debug mode. You should not see this in production.")

local start_urls_inverted = {}
for _, v in pairs(start_urls) do
  start_urls_inverted[v] = true
end

-- Function to be called whenever an item's download ends.
end_of_item = function()
  last_id_result = nil
  previously_queued_date = nil

end

set_new_item = function(url)
  if url == start_urls[next_start_url_index] then
    end_of_item()
    current_item_type = items_table[next_start_url_index][1]
    current_item_value = items_table[next_start_url_index][2]
    next_start_url_index = next_start_url_index + 1
    print_debug("Setting CIT to " .. current_item_type)
    print_debug("Setting CIV to " .. current_item_value)
  end
  assert(current_item_type)
  assert(current_item_value)
  set_item_yet = true
end

discover_item = function(item_type, item_name)
  assert(item_type)
  assert(item_name)

  if not discovered_items[item_type .. ":" .. item_name] then
    print_debug("Queuing for discovery " .. item_type .. ":" .. item_name)
  end
  discovered_items[item_type .. ":" .. item_name] = true
end

add_ignore = function(url)
  if url == nil then -- For recursion
    return
  end
  if downloaded[url] ~= true then
    downloaded[url] = true
  else
    return
  end
  add_ignore(string.gsub(url, "^https", "http", 1))
  add_ignore(string.gsub(url, "^http:", "https:", 1))
  add_ignore(string.match(url, "^ +([^ ]+)"))
  local protocol_and_domain_and_port = string.match(url, "^([a-zA-Z0-9]+://[^/]+)$")
  if protocol_and_domain_and_port then
    add_ignore(protocol_and_domain_and_port .. "/")
  end
  add_ignore(string.match(url, "^(.+)/$"))
end

for ignore in io.open("ignore-list", "r"):lines() do
  add_ignore(ignore)
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  assert(parenturl ~= nil)

  if start_urls_inverted[url] then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  if string.match(url, "^https?://[^/]+%.radikal.ru/")
    or string.match(url, "^https?://radikal.ru/") then
    print_debug("allowing " .. url .. " from " .. parenturl)
    return true
  end

  return false
end


local is_image_resource_url = function(s)
  return string.match(s, "^https?://[is]%d+%.radikal.ru/.+t%.jpg$")
          or string.match(s, "^https?://[ur]%.foto%.radikal.ru/.+t%.jpg$")
end


wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  if downloaded[url] == true or addedtolist[url] == true then
    return false
  end
  -- No reason to allow this at this stage of the project
  --[[
  if allowed(url, parent["url"]) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  local function check(urla, force)
    assert(not force or force == true) -- Don't accidentally put something else for force
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.match(url, "^(.-)%.?$")
    url_ = string.gsub(url_, "&amp;", "&")
    url_ = string.match(url_, "^(.-)%s*$")
    url_ = string.match(url_, "^(.-)%??$")
    url_ = string.match(url_, "^(.-)&?$")
    -- url_ = string.match(url_, "^(.-)/?$") # Breaks dl.
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and (allowed(url_, origurl) or force) then
      table.insert(urls, { url=url_, headers={["Accept-Language"]="en-US,en;q=0.5"}})
      --set_derived_url(url_)
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    -- Being caused to fail by a recursive call on "../"
    if not newurl then
      return
    end
    if string.match(newurl, "\\[uU]002[fF]") then
      return checknewurl(string.gsub(newurl, "\\[uU]002[fF]", "/"))
    end
    if string.match(newurl, "^https?:////") then
      check((string.gsub(newurl, ":////", "://")))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check((string.gsub(newurl, "\\", "")))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, "/" .. newurl))
    end
  end

  local function load_html()
    if html == nil then
      html = read_file(file)
    end
    return html
  end
  
  local function queue_gallery_query(image_id_anchor, direction, pagesize, date_anchor)
    table.insert(urls, {url="https://radikal.ru/Img/GetGalleryPage",
                        post_data="AnchorId=" .. image_id_anchor .. "&Direct=" .. direction ..
                        "&Tag=&PageSize=" .. pagesize .. "&DateTimeAnchor=" .. date_anchor})
  end

  if current_item_type == 'idrange' then
    local first_id = string.match(current_item_value, "^(%d+)%-") -- Noninclusive
    local last_id = string.match(current_item_value, "%-(%d+)$") -- Inclusive
    assert(first_id and last_id)
    if url == "https://this-is-a.dummy-site.jaa-wants-the-tld.invalid/" .. current_item_value then
      -- Start URL
      queue_gallery_query(first_id, "0", tostring(PAGE_SIZE), "")
    elseif url == "https://radikal.ru/Img/GetGalleryPage" then
      local j = JSON:decode(load_html())

      -- Some asserts
      assert(not j["IsError"])
      assert(not j["UrlForRedirect"]) -- Don't know what this is for

      local imgs = j["Imgs"]
      local is_finished = false
      assert(#imgs == PAGE_SIZE)
      for i = #imgs, 1, -1 do -- Iterate through the response from finish to start, i.e. in order of increasing IDs
        local this_img = imgs[i]

        -- Break if it is finished
        if this_img["IdLong"] > tonumber(last_id) then
          is_finished = true
          break
        end

        if this_img["OwnerUrlPart"] then
          discover_item("user", this_img["OwnerUrlPart"])
        end

        -- Queue the thumbnail
        local t_type_thumbnail = this_img["PublicPrevUrl"]
        print_debug("Thumb is " .. t_type_thumbnail)
        assert(is_image_resource_url(t_type_thumbnail))
        check(this_img["PublicPrevUrl"])
      end
      -- Queue the next page
      if not is_finished then
        queue_gallery_query(j["LeftAnchor"], "0", tostring(PAGE_SIZE), "")
      end
    end
  end

  if current_item_type == 'daterange' then
    local start_date = tonumber(string.match(current_item_value, "^(%d+)%-")) -- Noninclusive
    local stop_date = tonumber(string.match(current_item_value, "%-(%d+)$")) -- Inclusive
    assert(start_date and stop_date)
    if url == "https://this-is-a.dummy-site.jaa-wants-the-tld.invalid/d" .. current_item_value then
      -- Start URL
      queue_gallery_query("", "0", "1", tostring(start_date) .. "000")
      previously_queued_date = start_date
    elseif url == "https://radikal.ru/Img/GetGalleryPage" then
      local j = JSON:decode(load_html())
      assert(not j["IsError"])
      assert(not j["UrlForRedirect"]) -- Don't know what this is for

      local this_id_result = j["Imgs"][1]["IdLong"]
      print_debug("This ID result is " .. this_id_result)

      if last_id_result then
        if (this_id_result ~= last_id_result) then
          assert(this_id_result > last_id_result)
          discover_item("idrange", tostring(last_id_result) .. "-" .. tostring(this_id_result))
        end
      end -- Else this is after the first iteration

      last_id_result = this_id_result


      -- Put AFTER we queue the stuff
      if previously_queued_date < stop_date then
        local next_date = math.min(previously_queued_date + DATE_SLICE_TARGET_SECONDS, stop_date)
        -- Queue the next
        print_debug("Queueing at " .. tostring(next_date) .. "000")
        queue_gallery_query("", "0", "1", tostring(next_date) .. "000")
        previously_queued_date = next_date
      end

    end
  end

  if not status_code == 404 and is_image_resource_url(url["url"]) then
    assert(not string.match(load_html(), "<html"))
  end

  if status_code == 200 and not (string.match(url, "%.jpe?g$") or string.match(url, "%.png$")) then
    -- Completely disabled because I can't be bothered
    --[[load_html()

    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end]]
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  if not set_item_yet then
    set_new_item(url["url"])
  end
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()


  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
  end

  assert(not (string.match(url["url"], "^https?://[^/]*google%.com/sorry") or string.match(url["url"], "^https?://consent%.google%.com/")))

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

    --[[
  -- Handle redirects not in download chains
  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    print_debug("newloc is " .. newloc)
    if downloaded[newloc] == true or addedtolist[newloc] == true then
      tries = 0
      print_debug("Already encountered newloc " .. newloc)
      tries = 0
      return wget.actions.EXIT
    elseif not allowed(newloc, url["url"]) then
      print_debug("Disallowed URL " .. newloc)
      -- Continue on to the retry cycle
    else
      tries = 0
      print_debug("Following redirect to " .. newloc)
      assert(not (string.match(newloc, "^https?://[^/]*google%.com/sorry") or string.match(newloc, "^https?://consent%.google%.com/")))
      assert(not string.match(url["url"], "^https?://drive%.google%.com/file/d/.*/view$")) -- If this is a redirect, it will mess up initialization of file: items
      assert(not string.match(url["url"], "^https?://drive%.google%.com/drive/folders/[0-9A-Za-z_%-]+/?$")) -- Likewise for folder:

      addedtolist[newloc] = true
      return wget.actions.NOTHING
    end
  end]]

  local do_retry = false
  local maxtries = 12
  local url_is_essential = true

  local is_valid_0 = url["url"] == "https://this-is-a.dummy-site.jaa-wants-the-tld.invalid/" .. current_item_value
                  or url["url"] == "https://this-is-a.dummy-site.jaa-wants-the-tld.invalid/d" .. current_item_value
  local is_valid_404 = is_image_resource_url(url["url"])

  -- Whitelist instead of blacklist status codes
  if status_code ~= 200
    and not (status_code == 0 and is_valid_0)
    and not (status_code == 404 and is_valid_404) then
    print("Server returned " .. http_stat.statcode .. " (" .. err .. "). Sleeping.\n")
    do_retry = true
  end

  if do_retry then
    if tries >= maxtries then
      print("I give up...\n")
      tries = 0
      if not url_is_essential then
        return wget.actions.EXIT
      else
        print("Failed on an essential URL, aborting...")
        return wget.actions.ABORT
      end
    else
      sleep_time = math.floor(math.pow(2, tries))
      tries = tries + 1
    end
  end

  if do_retry and sleep_time > 0.001 then
    print("Sleeping " .. sleep_time .. "s")
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0
  return wget.actions.NOTHING
end


queue_list_to = function(list, key)
  if do_debug then
    for item, _ in pairs(list) do
      print("Would have sent discovered item " .. item)
    end
  else
    local to_send = nil
    for item, _ in pairs(list) do
      assert(string.match(item, ":")) -- Message from EggplantN, #binnedtray (search "colon"?)
      if to_send == nil then
        to_send = item
      else
        to_send = to_send .. "\0" .. item
      end
      print("Queued " .. item)
    end

    if to_send ~= nil then
      local tries = 0
      while tries < 10 do
        local body, code, headers, status = http.request(
          "http://blackbird-amqp.meo.ws:23038/" .. key .. "/",
          to_send
        )
        if code == 200 or code == 409 then
          break
        end
        os.execute("sleep " .. math.floor(math.pow(2, tries)))
        tries = tries + 1
      end
      if tries == 10 then
        abortgrab = true
      end
    end
  end
end


wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  queue_list_to(discovered_items, "radikal-y2nascr1jaeaje2")
  end_of_item()
end

wget.callbacks.write_to_warc = function(url, http_stat)
  set_new_item(url["url"])
  return true
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

