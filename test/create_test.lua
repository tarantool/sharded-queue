local t = require('luatest')
local g = t.group('create_test')

local helper = require('test.helper')

g.before_all(function()
    g.storage = helper.get_evaler('queue-storage-1-0')
end)

for test_name, options in pairs({
    none = {},
    fifo = {
        driver = 'sharded_queue.drivers.fifo',
    },
    fifottl = {
        driver = 'sharded_queue.drivers.fifottl',
    },
}) do
    g['test_create_tube_defauls_' .. test_name] = function()
        local tube_name = 'creates_tube_defaults_' .. test_name .. '_test'
        helper.create_tube(tube_name, options)

        local space = g.storage:eval(string.format([[
            local space = box.space.%s
            return {
                temporary = space.temporary,
                engine = space.engine,
            }
        ]], tube_name))
        t.assert_not(space.temporary)
        t.assert_equals(space.engine, 'memtx')
    end
end

for test_name, options in pairs({
    none_temporary = {
        temporary = true,
    },
    none_engine = {
        engine = 'vinyl',
    },
    fifo_temporary = {
        driver = 'sharded_queue.drivers.fifo',
        temporary = true,
    },
    fifo_engine = {
        driver = 'sharded_queue.drivers.fifo',
        engine = 'vinyl',
    },
    fifottl_temporary = {
        driver = 'sharded_queue.drivers.fifottl',
        temporary = true,
    },
    fifottl_engine = {
        driver = 'sharded_queue.drivers.fifottl',
        engine = 'vinyl',
    },
}) do
    g['test_create_tube_opts' .. test_name] = function()
        local tube_name = 'create_tube_opts_' .. test_name .. '_test'
        helper.create_tube(tube_name, options)

        local space = g.storage:eval(string.format([[
            local space = box.space.%s
            return {
                temporary = space.temporary,
                engine = space.engine,
            }
        ]], tube_name))
        t.assert_equals(space.temporary, options.temporary or false)
        t.assert_equals(space.engine, options.engine or 'memtx')
    end
end
