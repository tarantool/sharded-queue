local t = require('luatest')
local g = t.group('drop_test')

local helper = require('test.helper')
local utils = require('test.helper.utils')

g.before_all(function()
    g.queue_conn = helper.get_evaler('queue-router')
end)

function g.test_drop_empty()
    local tube_name = 'drop_empty_test'

    helper.create_tube(tube_name)
    helper.drop_tube(tube_name)

    local cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat, nil)
end

function g.test_drop_and_recreate()
    local tube_name = 'drop_and_recreate_test'

    helper.create_tube(tube_name)
    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { '*' } )

    helper.drop_tube(tube_name)

    -- Recreate tube with same name.
    helper.create_tube(tube_name)

    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { '*' } )
    t.assert_equals(task[utils.index.data], '*')
    t.assert_equals(task[utils.index.status], utils.state.READY)
end
