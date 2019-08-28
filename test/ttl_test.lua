local t = require('luatest')
local g = t.group('ttl_test')

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')

g.before_all = function()
    g.queue_conn = config.cluster:server('queue-router').net_box
end

local function shape_cmd(tube_name, cmd)
    return string.format('shared_queue.tube.%s:%s', tube_name, cmd)
end

function g.test_touch_task()
    local tube_name = 'touch_task_test'
    g.queue_conn:call('shared_queue.create_tube', {
        tube_name
    })

    local task = g.queue_conn:call(shape_cmd(tube_name, 'put'), {
        'simple data',
        { ttl = 0.2, ttr = 0.1 }
    })
    
    local peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.READY)
    t.assert_equals(peek_task[utils.index.data], 'simple data')

    local touched_task = g.queue_conn:call(shape_cmd(tube_name, 'touch'), {
        task[utils.index.task_id], 0.8
    })

    t.assert_equals(
        utils.round(tonumber(utils.sec(touched_task[utils.index.ttl])), 0.01), 1)
    fiber.sleep(0.5)

    local taken_task = g.queue_conn:call(shape_cmd(tube_name, 'take'))
    t.assert_equals(taken_task[utils.index.task_id], task[utils.index.task_id])

    peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), { task[utils.index.task_id] })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)        
end


function g.test_delayed_tasks()
    local tube_name = 'delayed_tasks_test'
    g.queue_conn:call('shared_queue.create_tube', {
        tube_name
    })
    -- task delayed for 0.1 sec 
    local task = g.queue_conn:call(shape_cmd(tube_name, 'put'), {
        'simple data',
        { delay = 1, ttl = 1, ttr = 0.1 }
    })
    
    local peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })

    -- delayed task was not taken
    t.assert_equals(peek_task[utils.index.status], utils.state.DELAYED)
    t.assert_equals(g.queue_conn:call(shape_cmd(tube_name, 'take'), { 0.001 }), nil)

    -- delayed task was taken after timeout
    local taken_task = g.queue_conn:call(shape_cmd(tube_name, 'take'), { 1 })
    t.assert_equals(taken_task[utils.index.data], 'simple data')

    peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)

    -- retake task before ttr
    t.assert_equals(g.queue_conn:call(shape_cmd(tube_name, 'take'), { 0.01 }), nil)

    fiber.sleep(0.09)
    peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.READY)

    -- retake task after ttr
    t.assert_equals(g.queue_conn:call(shape_cmd(tube_name, 'take'), { 0.1 })[utils.index.data], 'simple data')

    peek_task = g.queue_conn:call(shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)

end
