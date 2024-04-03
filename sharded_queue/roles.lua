local config = require('config')
local utils = require('sharded_queue.utils')

local function is_sharding_role_enabled(expected)
    local sharding_roles = config:get('sharding.roles')
    for _, role in ipairs(sharding_roles or {}) do
        if role == expected then
            return true
        end
    end
    return false
end

local function is_tarantool_role_supported()
    local major, minor, patch = utils.get_tarantool_version()
    if major <= 2
        or major == 3 and minor == 0 and patch <= 1 then
        return false
    end
    return true
end

local function get_tarantool_role_name(role)
    return "roles.sharded-queue-" .. role
end

return {
    get_tarantool_role_name = get_tarantool_role_name,
    is_sharding_role_enabled = is_sharding_role_enabled,
    is_tarantool_role_supported = is_tarantool_role_supported,
}
