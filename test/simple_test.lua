local t = require('luatest')
local g = t.group('simple_test')

local helper = require('test.helper')
local utils = require('test.helper.utils')

g.before_all(function()
    g.queue_conn = helper.get_evaler('queue-router')
end)

for test_name, options in pairs({
    fifottl = {},
    fifo = {
        temporary = true,
        driver = 'sharded_queue.drivers.fifo'
    }
}) do
    g['test_put_taken_' .. test_name] = function()
        local tube_name = 'put_taken_test_' .. test_name

        helper.create_tube(tube_name, options)

        -- tasks data for putting
        local task_count = 100
        local tasks_data = {}
        for i = 1, task_count do
            table.insert(tasks_data, {
                name = 'task_' .. i,
                raw = '*'
            })
        end
        -- returned tasks
        local task_ids = {}
        for _, data in pairs(tasks_data) do
            local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
            local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'),
                    {
                        task[utils.index.task_id]
                    })
            t.assert_equals(peek_task[utils.index.status], utils.state.READY)
            task_ids[task[utils.index.task_id]] = true
        end
        -- try taken this tasks
        local taken_task_ids = {}
        for _, _ in pairs(task_ids) do
            local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
            local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
                task[utils.index.task_id]
            })
            t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)
            taken_task_ids[task[utils.index.task_id]] = true
        end
        -- compare
        local stat = g.queue_conn:call('queue.statistics', { tube_name })
        t.assert_equals(stat.tasks.ready, 0)
        t.assert_equals(stat.tasks.taken, task_count)

        t.assert_equals(stat.calls.put, task_count)
        t.assert_equals(stat.calls.take, task_count)

        for task_id, _ in pairs(task_ids) do
            g.queue_conn:call(utils.shape_cmd(tube_name, 'ack'), {task_id})
        end

        t.assert_equals(task_ids, taken_task_ids)
    end
end

g.test_take_with_options = function()
    local tube_name = 'test_take_with_options'
    helper.create_tube(tube_name,
        {
            temporary = true,
            driver = 'sharded_queue.drivers.fifo',
        }
    )
    local options, timeout, data = {}, 1, 'data'

    for _, take_args in pairs({{}, {timeout}, {timeout, options}, {box.NULL, options}}) do
        g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), take_args)
        t.assert_equals(task[utils.index.data], data)
    end
end

function g.test_invalid_driver()
    t.assert_error_msg_contains('Driver unexistent could not be loaded', function() helper.create_tube(
        'invalid',
        {
            driver = 'unexistent'
        }
    )
    end)
    helper.drop_tube('invalid')
end

function g.test_delete()
    local tube_name = 'delete_test'
    helper.create_tube(tube_name)

    -- task data for putting
    local task_count = 20
    local tasks_data = {}

    for i = 1, task_count do
        table.insert(tasks_data, {
            name = 'task_' .. i,
            raw = '*'
        })
    end

    -- returned tasks
    local task_ids = {}
    for _, data in pairs(tasks_data) do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
        local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
            task[utils.index.task_id]
        })
        t.assert_equals(peek_task[utils.index.status], utils.state.READY)
        table.insert(task_ids, task[utils.index.task_id])
    end

    -- delete few tasks
    local deleted_tasks_count = 10
    local deleted_tasks = {}

    for i = 1, deleted_tasks_count do
        table.insert(deleted_tasks,
            g.queue_conn:call(utils.shape_cmd(tube_name, 'delete'), { task_ids[i] })[utils.index.task_id])
    end

    -- taken tasks
    local taken_task_ids = {}
    for _ = 1, task_count - deleted_tasks_count do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
        local peek_task = g.queue_conn:call(utils.shape_cmd(tube_name, 'peek'), {
            task[utils.index.task_id]
        })
        t.assert_equals(peek_task[utils.index.status], utils.state.TAKEN)
        taken_task_ids[task[utils.index.task_id]] = true
    end
    --
    local excepted_task_ids = {}
    for i = deleted_tasks_count + 1, #task_ids do
        excepted_task_ids[task_ids[i]] = true
    end

    -- compare

    local stat = g.queue_conn:call('queue.statistics', { tube_name })

    t.assert_equals(stat.tasks.ready, 0)
    t.assert_equals(stat.tasks.taken, task_count - deleted_tasks_count)

    t.assert_equals(stat.calls.put, task_count)
    t.assert_equals(stat.calls.delete, deleted_tasks_count)

    t.assert_equals(excepted_task_ids, taken_task_ids)
end

function g.test_release()
    local tube_name = 'release_test'
    helper.create_tube(tube_name)

    local task_count = 10
    local tasks_data = {}

    for i = 1, task_count do
        table.insert(tasks_data, {
            name = 'task_' .. i,
            raw = '*'
        })
    end

    -- returned tasks
    local task_ids = {}
    for _, data in pairs(tasks_data) do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
        t.assert_equals(task[utils.index.status], utils.state.READY)
        task_ids[task[utils.index.task_id]] = true
    end

    -- take few tasks
    local taken_task_count = 5
    local taken_task_ids = {}

    for _ = 1, taken_task_count do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
        t.assert_equals(task[utils.index.status], utils.state.TAKEN)
        taken_task_ids[task[utils.index.task_id]] = true
    end

    t.assert_covers(task_ids, taken_task_ids)

    for task_id, _ in pairs(taken_task_ids) do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'release'), { task_id })
        t.assert_equals(task[utils.index.status], utils.state.READY)
    end

    local result_task_id = {}

    for _ = 1, task_count do
        local task_id = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))[utils.index.task_id]
        result_task_id[task_id] = true
    end

    local stat = g.queue_conn:call('queue.statistics', { tube_name })

    t.assert_equals(stat.tasks.ready, 0)
    t.assert_equals(stat.tasks.taken, task_count)

    t.assert_equals(stat.calls.put, task_count)
    t.assert_equals(stat.calls.release, taken_task_count)
    t.assert_equals(stat.calls.take, task_count + taken_task_count)

    t.assert_equals(task_ids, result_task_id)
end

function g.test_bury_kick()
    local tube_name = 'bury_kick_test'
    helper.create_tube(tube_name)

    local cur_stat

    local task_count = 10

    -- returned tasks
    local task_ids = {}
    for i = 1, task_count do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { i })
        table.insert(task_ids, task[utils.index.task_id])
    end

    -- bury few task
    local bury_task_count = 5
    for i = 1, bury_task_count do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'bury'), { task_ids[i] })
        t.assert_equals(task[utils.index.status], utils.state.BURIED)
    end

    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.buried, bury_task_count)
    t.assert_equals(cur_stat.tasks.ready, task_count - bury_task_count)

    -- try unbury few task > bury_task_count
    local kick_cmd = utils.shape_cmd(tube_name, 'kick')
    t.assert_equals(g.queue_conn:call(kick_cmd, {bury_task_count + 3}), bury_task_count)

    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.calls.kick, bury_task_count)
    t.assert_equals(cur_stat.tasks.ready, task_count)
end
