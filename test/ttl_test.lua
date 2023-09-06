local t = require('luatest')
local g = t.group('ttl_test')

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')

g.before_all(function()
    g.queue_conn = config.cluster:server('queue-router').net_box
end)

local function lookup_task(task_id, tube_name, cluster)
    local call_string = ("return box.space.%s:get(%s):tomap({names_only=true})"):format(tube_name, task_id)
    local ok, stored_task
    for _, server in pairs(cluster.servers) do
        ok, stored_task = pcall(function()
            return server.net_box:eval(call_string)
        end)
        if ok then break end
    end
    return stored_task
end

function g.test_fifottl_config()
    local tube_name = 'test_fifottl_config'
    local tube_options = { ttl = 43, ttr = 15, priority = 17, wait_factor = 1 }
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        tube_options
    })

    local task_id = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
    })[1]

    local stored_task = lookup_task(task_id, tube_name, config.cluster)

    t.assert_equals(stored_task.ttl, tube_options.ttl * 1000000)
    t.assert_equals(stored_task.ttr, tube_options.ttr * 1000000)
    t.assert_equals(stored_task.priority, tube_options.priority)
end

function g.test_fifottl_config_pri()
    local tube_name = 'test_fifottl_config_pri'
    local tube_options = { ttl = 43, ttr = 15, pri = 15, wait_factor = 1 }
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        tube_options
    })

    local task_id = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
    })[1]

    local stored_task = lookup_task(task_id, tube_name, config.cluster)
    t.assert_equals(stored_task.priority, tube_options.pri)

    task_id = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple data', {pri = 18}})[1]
    stored_task = lookup_task(task_id, tube_name, config.cluster)
    t.assert_equals(stored_task.priority, 18)
end

function g.test_touch_task()
    local tube_name = 'touch_task_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 1,
        }
    })

    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
        { ttl = 0.2, ttr = 0.1 }
    })

    local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.READY)
    t.assert_equals(peek_task[utils.index.data], 'simple data')

    g.queue_conn:call(utils.shape_cmd(tube_name, 'touch'), {
        task[utils.index.task_id], 0.8
    })

    fiber.sleep(0.5)

    local taken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
    t.assert_equals(taken_task[utils.index.task_id], task[utils.index.task_id])

    peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), { task[utils.index.task_id] })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)

    local cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.calls.touch, 1)
end

function g.test_delayed_tasks()
    local tube_name = 'delayed_tasks_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 1,
        }
    })
    -- task delayed for 0.1 sec
    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
        { delay = 1, ttl = 1, ttr = 0.1 }
    })

    local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })

    -- delayed task was not taken
    t.assert_equals(peek_task[utils.index.status], utils.state.DELAYED)
    t.assert_equals(g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { 0.001 }), nil)

    -- delayed task was not taken
    fiber.sleep(0.5)
    t.assert_equals(g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { 0.01 }), nil)

    -- delayed task was taken after timeout
    local taken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { 0.5 })
    t.assert_equals(taken_task[utils.index.data], 'simple data')

    peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)

    -- retake task before ttr
    t.assert_equals(g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { 0.01 }), nil)

    fiber.sleep(0.09)
    peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.READY)

    -- retake task after ttr
    local take_cmd = utils.shape_cmd(tube_name, 'take')
    t.assert_equals(g.queue_conn:call(take_cmd, { 0.1 })[utils.index.data], 'simple data')

    peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
        task[utils.index.task_id]
    })
    t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)

end

function g.test_ttr_release_no_delete_task()
    local tube_name = 'ttr_release_no_delete_task_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 1,
            ttr = 0.2,
            log_request = true,
        }
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
    })
    local taken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
    local released_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'release'), {
        taken_task[utils.index.task_id]
    })
    t.assert_equals(released_task[utils.index.data], 'simple data')
    t.assert_equals(released_task[utils.index.status], utils.state.READY)

    -- Wait for a clenup fiber.
    fiber.sleep(1)

    local retaken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), {0.5})
    t.assert_not_equals(retaken_task, box.NULL)
    t.assert_equals(retaken_task[utils.index.data], 'simple data')
    t.assert_equals(retaken_task[utils.index.status], utils.state.TAKEN)
end

function g.test_ttr_bury_no_delete_task()
    local tube_name = 'ttr_bury_no_delete_task_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 1,
            ttr = 0.2,
            log_request = true,
        }
    })

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
    })
    local taken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
    local buried_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'bury'), {
        taken_task[utils.index.task_id]
    })
    t.assert_equals(buried_task[utils.index.data], 'simple data')
    t.assert_equals(buried_task[utils.index.status], utils.state.BURIED)

    -- Wait for a clenup fiber.
    fiber.sleep(1)

    g.queue_conn:call(utils.shape_cmd(tube_name, 'kick'), {1})
    local retaken_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), {0.5})
    t.assert_not_equals(retaken_task, box.NULL)
    t.assert_equals(retaken_task[utils.index.data], 'simple data')
    t.assert_equals(retaken_task[utils.index.status], utils.state.TAKEN)
end
