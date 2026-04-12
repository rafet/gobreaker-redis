package respstore

// luaCAS is the Lua script that implements optimistic concurrency control on
// a single Snapshot key.
//
// Inputs:
//
//	KEYS[1]   = full Redis key for the Snapshot HASH
//	ARGV[1]   = expected version (decimal string). The string "0" is used
//	            both for "version 0 (genuinely fresh)" and for "key does not
//	            yet exist": both encode the same precondition.
//	ARGV[2]   = new version (decimal string)
//	ARGV[3]   = TTL in milliseconds, or "0" to leave TTL unchanged
//	ARGV[4..] = alternating field, value pairs to write into the HASH
//	            (must NOT include the version field; the script writes it
//	            from ARGV[2])
//
// Returns a two-element array:
//
//	{1, <persisted version>}        on success
//	{0, <current version on disk>}  on CAS conflict
//
// The script runs as a single atomic unit, so concurrent processes contend
// safely on the same key without an external lock.
//
// We deliberately do not use HMSET (deprecated since Redis 4.0) and do not
// rely on Redis 7+ features. The script targets Redis 6+, Valkey 7+, KeyDB
// 6+, and DragonflyDB 1.0+. miniredis (which embeds gopher-lua) also runs it
// for unit tests.
const luaCAS = `
local key = KEYS[1]
local expected = ARGV[1]
local newVersion = ARGV[2]
local ttlMs = tonumber(ARGV[3]) or 0

local current = redis.call('HGET', key, 'v')
if current == false then
    current = "0"
end

if current ~= expected then
    return {0, current}
end

-- ARGV[4..] is field, value, field, value, ...
-- Build the HSET argument list: version first, then everything else.
local hsetArgs = {'v', newVersion}
for i = 4, #ARGV do
    hsetArgs[#hsetArgs + 1] = ARGV[i]
end
redis.call('HSET', key, unpack(hsetArgs))

if ttlMs > 0 then
    redis.call('PEXPIRE', key, ttlMs)
end

return {1, newVersion}
`
