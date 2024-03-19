---- Module for preserving data between cartridge role reloads.
--
-- Based on: https://github.com/tarantool/crud/blob/781f3c46eb14d50449d34099354c59d0e4d6fae2/crud/common/stash.lua

local is_hotreload_supported, hotreload = pcall(require, "cartridge.hotreload")

local stash = {}

function stash.setup(names)
    if not is_hotreload_supported then
        return
    end

    for _, name in pairs(names) do
        hotreload.whitelist_globals({ name })
    end
end

function stash.get(name)
    local instance = rawget(_G, name) or {}
    rawset(_G, name, instance)

    return instance
end

return stash
