-- Fixed-window request counter; per-second key shared across workers.
-- KEYS[1] = window key prefix
-- returns: call count in the current second

local t = redis.call('TIME')
local key = KEYS[1] .. ':' .. t[1]
local n = redis.call('INCR', key)
if n == 1 then redis.call('EXPIRE', key, 2) end
return n
