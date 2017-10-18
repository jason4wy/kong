-- Copyright (C) Kong Inc.

local BasePlugin = require "kong.plugins.base_plugin"

local math_random = math.random
local math_floor = math.floor
local math_fmod = math.fmod
local crc32 = ngx.crc32_short

local log_prefix = "[cannary] "

local Canary = BasePlugin:extend()

function Canary:new()
  Canary.super.new(self, "canary")
end

local function get_hash(hash)
  local ctx = ngx.ctx
  local identifier

  if hash == "consumer" then
    -- Consumer is identified id
    identifier = ctx.authenticated_consumer and ctx.authenticated_consumer.id
    if not identifier and ctx.authenticated_credential then
      -- Fallback on credential
      identifier = ctx.authenticated_credential.id
    end
  end

  if not identifier then
    -- remote IP
    identifier = ngx.var.remote_addr
    if not identifier then
      -- Fallback on a random number
      identifier = tostring(math_random())
    end
  end

  return crc32(identifier)
end

local conf_cache = setmetatable({},{__mode = "k"})

function Canary:access(conf)
  Canary.super.access(self)
  
  local start, steps, duration = conf.start, conf.steps, conf.duration
  local time = ngx.now()

  if time < start then
    -- not started yet, exit
    return
  end

  local run_conf = conf_cache[conf]
  if not run_conf then
    run_conf = {}
    conf_cache[conf] = run_conf
    run_conf.prefix = log_prefix .. ngx.ctx.balancer_address.host ..
       "->" .. conf.target_host .. " "
    run_conf.step = 0
    ngx.log(ngx.WARN, run_conf.prefix, run_conf.step, "/",
                      conf.steps, " starting canary")
  end

  if time > start + duration then
    -- completely done, switch target
    if not run_conf.complete then
      run_conf.complete = true
      run_conf.step = conf.steps
      ngx.log(ngx.WARN, run_conf.prefix, run_conf.step, "/",
                        conf.steps, " completed canary")
    end
    ngx.ctx.balancer_address.host = conf.target_host
    return
  end

  -- calculate current step, and hash position. Both 0-indexed.
  local step = math_floor((time - start) / duration * steps)
  local hash = math_fmod(get_hash(conf.hash), steps)

  if step ~= run_conf.step then
    run_conf.step = step
    ngx.log(ngx.DEBUG, run_conf.prefix, step, "/", conf.steps)
  end

  if hash <= step then
    -- switch upstream host to the new hostname
    ngx.ctx.balancer_address.host = conf.target_host
  end
end

Canary.PRIORITY = 13

return Canary
