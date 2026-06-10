-- Atomic token-bucket step for a distributed (aggregate) rate limiter.
-- KEYS[1] = bucket key
-- ARGV = rate (tokens/sec), capacity, requested tokens
-- returns: 0 if granted, else milliseconds to wait before retrying

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + (now - ts) * rate)

local wait_ms = 0
if tokens >= requested then
  tokens = tokens - requested
else
  wait_ms = math.ceil(((requested - tokens) / rate) * 1000)
  if wait_ms < 1 then wait_ms = 1 end
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
-- Self-clean: expire once enough idle time has passed to refill fully.
redis.call('PEXPIRE', key, math.ceil((capacity / rate) * 1000) + 1000)
return wait_ms
