local t = require('luatest')
local g = t.group('storage')

local helper = require('test.helper')
local methods = require('sharded_queue.storage.methods')

g.before_all(function()
    g.storage_master = helper.get_evaler('queue-storage-1-0')
    g.storage_ro = helper.get_evaler('queue-storage-1-1')
end)

g.test_storage_methods = function()
    --make sure storage_ro is read_only
    local ro = g.storage_ro:eval("return box.cfg.read_only")
    t.assert_equals(ro, true)

    for _, method  in pairs(methods.get_list()) do
        local global_name = 'tube_' .. method
        -- Master storage
       t.assert_equals(g.storage_master:eval(string.format('return box.schema.func.exists("%s")', global_name)), true)
       --Read Only storage
       t.assert_equals(g.storage_ro:eval(string.format('return box.schema.func.exists("%s")', global_name)), true)
    end
end
