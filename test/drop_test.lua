local t = require('luatest')
local g = t.group('drop_test')

local config = require('test.helper.config')
local utils = require('test.helper.utils')

g.before_all = function()
    g.queue_conn = config.cluster:server('queue-router').net_box
end

local function shape_cmd(tube_name, cmd)
    return string.format('queue.tube.%s:%s', tube_name, cmd)
end

function g.test_drop_empty()
    local tube_name = 'drop_empty_test'
    
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })
    g.queue_conn:call(shape_cmd(tube_name, 'drop'))

    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat, nil)
end

function g.test_drop_and_recreate()
    local tube_name = 'drop_and_recreate_test'

    g.queue_conn:call('queue.create_tube', {
        tube_name
    })
    g.queue_conn:call(shape_cmd(tube_name, 'put'), { '*' } )

    g.queue_conn:call(shape_cmd(tube_name, 'drop'))

    -- recreate tube with same name
    t.assert(g.queue_conn:call('queue.create_tube', {
        tube_name
    }))

    local task = g.queue_conn:call(shape_cmd(tube_name, 'put'), { '*' } )
    t.assert_equals(task[utils.index.data], '*')
    t.assert_equals(task[utils.index.status], utils.state.READY)

end