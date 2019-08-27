local helper = require('test.helper')
local netbox = require('net.box')
local fiber = require('fiber')

local t = require('luatest')

local g = t.group('ttl_test')
---

g.before_all = function()
    queue_conn = netbox.connect('localhost:3301')
end

g.after_all = function()
    queue_conn:close()
end

---

function g.test_touch_task()
    local tube_name = 'touch_task_test'
    queue_conn:eval('tube = shared_queue.create_tube(...)', { tube_name })


    local task = queue_conn:call('tube:put', {
        'simple data',
        { ttl = 0.2, ttr = 0.1 }
    })
    
    local peek_task = queue_conn:call('tube:peek', {
        task[helper.index.task_id]
    })
    t.assert_equals(peek_task[helper.index.status], helper.state.READY)    
    t.assert_equals(peek_task[helper.index.data], 'simple data')

    local touched_task = queue_conn:call('tube:touch', {
        task[helper.index.task_id], 0.8
    })

    t.assert_equals(
        helper.round(tonumber(helper.sec(touched_task[helper.index.ttl])), 0.01), 1)
    fiber.sleep(0.5)

    local taken_task = queue_conn:call('tube:take')
    t.assert_equals(taken_task[helper.index.task_id], task[helper.index.task_id])

    peek_task = queue_conn:call('tube:peek', { task[helper.index.task_id] })
    t.assert_equals(peek_task[helper.index.status], helper.state.TAKEN)        
end


function g.test_delayed_tasks()
    local tube_name = 'delayed_tasks_test'
    queue_conn:eval('tube = shared_queue.create_tube(...)', { tube_name })

    -- task delayed for 0.1 sec 
    local task = queue_conn:call('tube:put', {
        'simple data',
        { delay = 1, ttl = 1, ttr = 0.1 }
    })
    
    local peek_task = queue_conn:call('tube:peek', {
        task[helper.index.task_id]
    })

    -- delayed task was not taken
    t.assert_equals(peek_task[helper.index.status], helper.state.DELAYED)
    t.assert_equals(queue_conn:call('tube:take', { 0.001 }), nil)

    -- delayed task was taken after timeout
    local taken_task = queue_conn:call('tube:take', { 1 })
    t.assert_equals(taken_task[helper.index.data], 'simple data')

    peek_task = queue_conn:call('tube:peek', {
        task[helper.index.task_id]
    })
    t.assert_equals(peek_task[helper.index.status], helper.state.TAKEN)

    -- retake task before ttr
    t.assert_equals(queue_conn:call('tube:take', { 0.01 }), nil)

    fiber.sleep(0.09)
    peek_task = queue_conn:call('tube:peek', {
        task[helper.index.task_id]
    })
    t.assert_equals(peek_task[helper.index.status], helper.state.READY)

    -- retake task after ttr
    t.assert_equals(queue_conn:call('tube:take', { 0.1 })[helper.index.data], 'simple data')

    peek_task = queue_conn:call('tube:peek', {
        task[helper.index.task_id]
    })
    t.assert_equals(peek_task[helper.index.status], helper.state.TAKEN)

end
