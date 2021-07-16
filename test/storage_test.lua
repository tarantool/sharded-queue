#!/usr/bin/env tarantool

local t = require('luatest')
local log = require('log')  -- luacheck: ignore
local g = t.group('storage')

local storage = require('sharded_queue.storage')
local config = require('test.helper.config')


g.before_all(function()
    g.storage_master = config.cluster:server('queue-storage-1-0').net_box
    g.storage_ro = config.cluster:server('queue-storage-1-1').net_box
end)

g.test_storage_methods = function()
    --make sure storage_ro is read_only
    local ro = g.storage_ro:eval("return box.cfg.read_only")
    t.assert_equals(ro, true)

    for _,method  in pairs(storage.__private.methods) do
        local global_name = 'tube_' .. method
        -- Master storage
       t.assert_equals(g.storage_master:eval(string.format('return box.schema.func.exists("%s")', global_name)), true)
       --Read Only storage
       t.assert_equals(g.storage_ro:eval(string.format('return box.schema.func.exists("%s")', global_name)), true)
    end
end
