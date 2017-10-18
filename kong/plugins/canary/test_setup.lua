local kong_admin = "127.0.0.1:8001"
local kong_proxy = "127.0.0.1:8000"



local http = require("resty.http")
local decode = require("cjson").decode
local encode = require("cjson").encode

--- Debug function for development purposes.
-- Will dump all passed in parameters in a pretty-printed way
-- as a `debug` log message. Includes color markers to make it stand out.
-- @param ... list of parameters to dump
local dump = function(...)
  local info = debug.getinfo(2) or {}
  local input = { n = select("#", ...), ...}
  local write = require("pl.pretty").write
  local serialized
  if input.n == 1 and type(input[1]) == "table" then
    serialized = "(" .. type(input[1]) .. "): " .. write(input[1])
  elseif input.n == 1 then
    serialized = "(" .. type(input[1]) .. "): " .. tostring(input[1]) .. "\n"
  else
    local n
    n, input.n = input.n, nil
    serialized = "(list, #" .. n .. "): " .. write(input)
  end

  ngx.log(ngx.WARN,
          "\027[31m\n",
          "function '", tostring(info.name), ":" , tostring(info.currentline),
          "' in '", tostring(info.short_src), "' wants you to know:\n",
          serialized,
          "\027[0m")
end

-- create a new client on each action, auto-retry on failures
local request = function(...)
  local res, err
  for i = 1,5 do 
    local client = http.new()
    res, err =  client:request_uri(...)
    if res then
      -- only return actual fields
      return { status = res.status, headers = res.headers, body = res.body }
    end
  end
  return nil, err
end

--- returns a list with apis. Each api also indexed by `name` and by `id`.
local apis_list = function()
  local res, err = request("http://"..kong_admin.."/apis", {

    method = "GET",
    body = "",
    headers = {
      ["Content-Type"] = "application/x-www-form-urlencoded",
    }
  })
  if not res or res.status ~= 200 then
    return nil, (res and res.status or "") .. (err and " " .. err or "")
  end
  local apis = decode(res.body).data
  for _, api in ipairs(apis) do
    apis[api.name] = api
    apis[api.id] = api
  end
  return apis
end

--- deletes an api.
-- @param api the api to delete, either a table, or a string
-- @return true on success, false if not found or nil+error
local api_delete = function(api)
  if not api then return nil, "no api provided" end
  if type(api) == "table" then api = api.id end
  local res, err = request("http://"..kong_admin.."/apis/"..api, {

    method = "DELETE",
    body = "",
    headers = {
      ["Content-Type"] = "application/x-www-form-urlencoded",
    }
  })
  if not res or (res.status ~= 404 and res.status ~= 204) then
    return nil, (res and res.status or "") .. (err and " " .. err or "")
  end
  return res.status == 204 
end

--- creates an api from a table.
-- @return table with api
local api_create = function(api)
  assert(type(api) == "table", "expected a table")
  local res, err = request("http://"..kong_admin.."/apis", {

    method = "POST",
    body = encode(api),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

--- Gets an api.
-- @param api api to fetch, table, or string holding id or name
-- @return table with api
local api_get = function(api)
  if type(api) == "table" then api = api.id or api.name end
  local res, err = request("http://"..kong_admin.."/apis/"..api, {
    method = "GET",
    body = "",
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 200) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

--- returns a list with apis. Each api also indexed by `name` and by `id`.
local consumers_list = function()
  local res, err = request("http://"..kong_admin.."/consumers", {

    method = "GET",
    body = "",
    headers = {
      ["Content-Type"] = "application/x-www-form-urlencoded",
    }
  })
  if not res or res.status ~= 200 then
    return nil, (res and res.status or "") .. (err and " " .. err or "")
  end
  local consumers = decode(res.body).data
  for _, consumer in ipairs(consumers) do
    consumers[consumer.username] = consumer
    consumers[consumer.id] = consumer
  end
  return consumers
end

--- deletes a consumer.
-- @param consumer the consumer to delete, either a table, or a string
-- @return true on success, false if not found or nil+error
local consumer_delete = function(consumer)
  if not consumer then return nil, "no consumer provided" end
  if type(consumer) == "table" then consumer = consumer.id end
  local res, err = request("http://"..kong_admin.."/consumers/"..consumer, {

    method = "DELETE",
    body = "",
    headers = {
      ["Content-Type"] = "application/x-www-form-urlencoded",
    }
  })
  if not res or (res.status ~= 404 and res.status ~= 204) then
    return nil, (res and res.status or "") .. (err and " " .. err or "")
  end
  return res.status == 204 
end

--- creates an consumer from a table.
-- @return table with consumer, or string with username
local consumer_create = function(consumer)
  if type(consumer) == "string" then consumer = { username = consumer } end
  assert(type(consumer) == "table", "expected a table or string")
  local res, err = request("http://"..kong_admin.."/consumers", {

    method = "POST",
    body = encode(consumer),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

--- Gets a consumer.
-- @param consumer consumer to fetch, table, or string holding id or username
-- @return table with consumer
local consumer_get = function(consumer)
  if type(consumer) == "table" then consumer = consumer.id or consumer.username end
  local res, err = request("http://"..kong_admin.."/consumers/"..consumer, {
    method = "GET",
    body = "",
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 200) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end


--- deletes an upstream.
-- @param upstream the upstream to delete, either a table, or a string
-- @return true on success, false if not found or nil+error
local upstream_delete = function(upstream)
  if not upstream then return nil, "no upstream provided" end
  if type(upstream) == "table" then upstream = upstream.id end
  local res, err = request("http://"..kong_admin.."/upstreams/"..upstream, {
    method = "DELETE",
    body = "",
    headers = {
      ["Content-Type"] = "application/x-www-form-urlencoded",
    }
  })
  if not res or (res.status ~= 404 and res.status ~= 204) then
    return nil, (res and res.status or "") .. (err and " " .. err or "")
  end
  return res.status == 204 
end

--- creates an upstream from a table.
-- @return table with upstream, or string with upstream name
local upstream_create = function(upstream)
  if type(upstream) == "string" then upstream = { name = upstream } end
  assert(type(upstream) == "table", "expected a table or string")
  local res, err = request("http://"..kong_admin.."/upstreams", {
    method = "POST",
    body = encode(upstream),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

--- Set upstream target.
-- @param upstream upstream table, id or name
-- @param target target table, string formatted as "<ip>:<port>"
-- @param weight (optional) weight to assign to the target, 0 to disable the target
local target_set = function(upstream, target, weight)
  if type(upstream) == "table" then upstream = upstream.id end
  if type(target) == "table" then target = target.target end
  local res, err = request("http://"..kong_admin.."/upstreams/"..upstream.."/targets", {
    method = "POST",
    body = encode({
        target = target,
        weight = weight,
      }),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end


--- create key-auth credential.
-- @param consumer consumer table, id or username
local keyauth_create = function(consumer, key)
  if type(consumer) == "table" then consumer = consumer.id end
  local res, err = request("http://"..kong_admin.."/consumers/"..consumer.."/key-auth", {

    method = "POST",
    body = encode({key = key}),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

--- create plugin.
-- @param plugin plugin name, or table
-- @param api (optional) api table, id or name
-- @param consumer (optional) consumer table, id or username
-- @return created plugin config
local plugin_create = function(plugin, api, consumer)
  if type(api) == "string" then api = assert(api_get(api)) end
  if type(consumer) == "string" then consumer = assert(consumer_get(consumer)) end
  if type(plugin) == "string" then plugin = { name = plugin } end
  if api then plugin.api_id = api.id end
  if consumer then plugin.consumer_id = consumer.id end
  local res, err = request("http://"..kong_admin.."/plugins/", {
    method = "POST",
    body = encode(plugin),
    headers = {
      ["Content-Type"] = "application/json",
    }
  })
  if not res or (res.status ~= 201) then
    return nil, (res and res.status or "") .. (err and " " .. err or (res and res.body))
  end
  return decode(res.body)
end

-- deletes all consumers
local consumer_clear = function()
  local consumers = assert(consumers_list())
  for _, consumer in ipairs(consumers) do
    assert(consumer_delete(consumer))
  end
end






upstream_delete("balancer_a")
upstream_create("balancer_a")
target_set("balancer_a", "127.0.0.1:8888")
target_set("balancer_a", "127.0.0.1:8889")

upstream_delete("balancer_b")
upstream_create("balancer_b")
target_set("balancer_b", "127.0.0.1:8890")
target_set("balancer_b", "127.0.0.1:8891")

api_delete("canary_test")
api_create({
    name = "canary_test",
    uris = "/",
    upstream_url = "http://balancer_a/",
  })

plugin_create("key-auth", "canary_test")

consumer_clear()
for i = 1, 1000 do
  local username = "user"..i;  consumer_create(username)
  local secret = "secret"..i;  keyauth_create(username, secret)
end





local now = ngx.now()
local start = now + 30
local done = now + 120

print("start load, ", now)
while ngx.now() < done do
  if ngx.now() > start then
    start = done + 1
    print("start canary ", start)
    dump(plugin_create({
        name = "canary",
        config = {
          target_host = "balancer_b",
          duration = 30,
        },
      }, "canary_test"))
  end
  local res, err = request("http://"..kong_proxy, {
    method = "GET",
    body = "",
    headers = {
      ["apikey"] = "secret"..math.random(1, 1000),
      ["Content-Type"] = "application/json",
    }
  })
ngx.sleep(0.05)
end
print("done ", done)

