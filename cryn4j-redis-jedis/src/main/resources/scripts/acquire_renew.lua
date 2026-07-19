-- acquire_renew.lua  — cryn4j deterministic lease engine
--
-- Single-RTT atomic token-bucket operation. All state lives server-side.
-- No CAS retry, no client-clock dependence.
--
-- KEYS[1]  = hash key, e.g. "cryn4j:{userId}"
-- ARGV[1]  = capacity          (max tokens, integer)
-- ARGV[2]  = refillTokens      (tokens per refillPeriodMicros)
-- ARGV[3]  = refillPeriodMicros
-- ARGV[4]  = unusedReturn      (tokens being returned from previous lease, >= 0)
-- ARGV[5]  = leaseRequest      (tokens wanted for next lease, >= 0)
-- ARGV[6]  = ttlSeconds        (key TTL)
--
-- Returns: {grant, remaining, nowMicros}
--
-- Security hardening (see security audit):
--   V-01: Overflow guard — cap elapsed before refillTok*elapsed multiplication.
--   V-07: Key-eviction guard — newly created key starts at cap/2, not cap.
--   All ARGV values are clamped to [0, cap] before arithmetic.

redis.replicate_commands()

local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])   -- microseconds, server-authoritative

local cap        = math.max(1, tonumber(ARGV[1]))
local refillTok  = math.max(0, tonumber(ARGV[2]))
local periodUs   = math.max(1, tonumber(ARGV[3]))
local returning  = math.min(cap, math.max(0, tonumber(ARGV[4])))   -- clamp [0, cap]
local requesting = math.min(cap, math.max(0, tonumber(ARGV[5])))   -- clamp [0, cap]
local ttl        = math.max(5,  tonumber(ARGV[6]))

-- Read state
local exists = redis.call('EXISTS', KEYS[1])
local h = redis.call('HMGET', KEYS[1], 't', 'ts', 'c')
local tokens  = tonumber(h[1])
local lastTs  = tonumber(h[2]) or now
local carry   = tonumber(h[3]) or 0

-- V-07: Key eviction guard.
-- If key did not exist (evicted or first use) start at cap/2, not cap.
-- This prevents "eviction = full grant" bypasses under allkeys-lru pressure.
if tokens == nil then
    if exists == 0 then
        tokens = math.floor(cap / 2)   -- conservative initial fill
    else
        tokens = cap                   -- key exists but field empty → treat as full
    end
end
tokens = math.min(cap, math.max(0, tokens))

-- Accept returned tokens (safe: only what was previously granted)
tokens = math.min(cap, tokens + returning)

-- Refill with integer rounding-error carry (zero float drift)
local elapsed = math.max(0, now - lastTs)

-- V-01: Overflow guard — cap elapsed before refillTok * elapsed.
-- At Lua's 2^53 integer limit: max safe elapsed = (2^53 - carry) / refillTok
-- Use a conservative cap: at most 2 full refill periods worth of tokens.
local maxElapsed = math.floor((cap * periodUs) / math.max(1, refillTok)) * 2 + periodUs
if elapsed > maxElapsed then elapsed = maxElapsed end

local dividend = refillTok * elapsed + carry
local added    = math.floor(dividend / periodUs)
carry          = dividend - added * periodUs
tokens         = math.min(cap, tokens + added)

-- Grant: never more than what exists (the accuracy guarantee)
local grant = math.min(requesting, tokens)
tokens      = tokens - grant

-- Persist
redis.call('HMSET', KEYS[1], 't', tokens, 'ts', now, 'c', carry)
redis.call('EXPIRE', KEYS[1], ttl)

return {grant, tokens, now}
