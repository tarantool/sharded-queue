local t = require('luatest')
local g = t.group('dlq_test')

local helper = require('test.helper')
local utils = require('test.helper.utils')

local consts = require('sharded_queue.consts')

g.before_all(function()
    g.queue_conn = helper.get_evaler('queue-router')
end)

for test_name, options in pairs({
    fifottl = {
        driver = 'sharded_queue.drivers.fifottl',
        release_limit = 5,
        release_limit_policy = consts.RELEASE_LIMIT_POLICY.DELETE,
    },
    fifo = {
        driver = 'sharded_queue.drivers.fifo',
        release_limit = 5,
        release_limit_policy = consts.RELEASE_LIMIT_POLICY.DELETE,
    },
    fifo_release_policy_nil = {
        driver = 'sharded_queue.drivers.fifo',
        release_limit = 5,
    },
}) do
    g['test_dlq_delete_' .. test_name] = function()
    local tube_name = 'test_dlq_delete_' .. test_name
    helper.create_tube(tube_name, options)

    local task_count = 10
    local tasks_data = {}

    for i = 1, task_count do
        table.insert(tasks_data, {
            name = 'task_' .. i,
            raw = '*'
        })
    end

    local task_ids = {}
    for _, data in pairs(tasks_data) do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
        t.assert_equals(task[utils.index.status], utils.state.READY)
        task_ids[task[utils.index.task_id]] = true
    end

    -- Release tasks several times to reach 'release_limit'.
    for _ = 1, options.release_limit do
        local taken_task_ids = {}

        for _ = 1, task_count do
            local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
            t.assert_equals(task[utils.index.status], utils.state.TAKEN)
            taken_task_ids[task[utils.index.task_id]] = true
        end
        t.assert_equals(task_ids, taken_task_ids)

        for task_id, _ in pairs(taken_task_ids) do
            g.queue_conn:call(utils.shape_cmd(tube_name, 'release'), { task_id })
        end
    end

    -- All tasks should be deleted.
    local stat = g.queue_conn:call('queue.statistics', { tube_name })

    t.assert_equals(stat.calls.delete, task_count)
    end
end

for test_name, options in pairs({
    fifottl = {
        driver = 'sharded_queue.drivers.fifottl',
        release_limit = 2,
        release_limit_policy = consts.RELEASE_LIMIT_POLICY.DLQ,
    },
    fifo = {
        driver = 'sharded_queue.drivers.fifo',
        release_limit = 2,
        release_limit_policy = consts.RELEASE_LIMIT_POLICY.DLQ,
    }
}) do
    g['test_dlq_move_' .. test_name] = function()
    local tube_name = 'test_dlq_move_' .. test_name
    helper.create_tube(tube_name, options)

    local task_count = 10
    local tasks_data = {}

    for i = 1, task_count do
        table.insert(tasks_data, {
            name = 'task_' .. i,
            raw = '*'
        })
    end

    local task_ids = {}
    for _, data in pairs(tasks_data) do
        local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { data })
        t.assert_equals(task[utils.index.status], utils.state.READY)
        task_ids[task[utils.index.task_id]] = true
    end

    -- Release tasks several times to reach 'release_limit'.
    for _ = 1, options.release_limit do
        local taken_task_ids = {}

        for _ = 1, task_count do
            local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
            t.assert_equals(task[utils.index.status], utils.state.TAKEN)
            taken_task_ids[task[utils.index.task_id]] = true
        end
        t.assert_equals(task_ids, taken_task_ids)

        for task_id, _ in pairs(taken_task_ids) do
            g.queue_conn:call(utils.shape_cmd(tube_name, 'release'), { task_id })
        end
    end

    -- All tasks should be deleten and put into DLQ.
    local stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(stat.calls.delete, task_count)
    local dlq_stat = g.queue_conn:call('queue.statistics', { tube_name .. consts.DLQ_SUFFIX })
    t.assert_equals(dlq_stat.calls.put, task_count)
    end
end

function g.test_ttr_release_move_dlq()
    local tube_name = 'test_ttr_release_move_dlq'
    helper.create_tube(
        tube_name,
        {
            driver = 'sharded_queue.drivers.fifottl',
            release_limit = 1,
            release_limit_policy = consts.RELEASE_LIMIT_POLICY.DLQ
        }
    )

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
        { ttl = 60, ttr = 0.1 }
    })
    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
    t.assert_equals(task[utils.index.data], 'simple data')
    t.assert_equals(task[utils.index.status], utils.state.TAKEN)

    -- Wait for a ttr.
    t.helpers.retrying({}, function()
        -- Task should be deleten because of release_limit = 1 and put into DLQ.
        local stat = g.queue_conn:call('queue.statistics', { tube_name })
        t.assert_equals(stat.calls.delete, 1)
        local dlq_stat = g.queue_conn:call('queue.statistics', { tube_name .. consts.DLQ_SUFFIX })
        t.assert_equals(dlq_stat.calls.put, 1)
    end)
end

function g.test_dlq_validation()
    t.skip_if(utils.is_tarantool_3(), 'the role is available only for Cartridge')
    local tube_name = 'test_dlq_validation'

    utils.try(function()
        local options = {
            driver = 'sharded_queue.drivers.fifottl',
            release_limit = 1,
            release_limit_policy = 'DLQfoo'
        }
        g.queue_conn:eval('queue.create_tube(...)', {tube_name, options})
    end, function(err)
        t.assert_str_contains(tostring(err), 'unknown release_limit_policy')
    end)

    utils.try(function()
        local options = {
            driver = 'sharded_queue.drivers.fifottl',
            release_limit = 1,
            release_limit_policy = 123
        }
        g.queue_conn:eval('queue.create_tube(...)', {tube_name, options})
    end, function(err)
        t.assert_str_contains(tostring(err), 'release_limit_policy must be string')
    end)
end

function g.test_dlq_validation_already_exists()
    t.skip_if(utils.is_tarantool_3(), 'the role is available only for Cartridge')
    local tube_name = 'test_dlq_validation_already_exists'
    local dlq_tube_name = tube_name .. consts.DLQ_SUFFIX

    utils.try(function()
        local options = {
            driver = 'sharded_queue.drivers.fifottl',
            release_limit = 1,
            release_limit_policy = consts.RELEASE_LIMIT_POLICY.DLQ
        }
        g.queue_conn:eval('queue.create_tube(...)', {dlq_tube_name, options})
        g.queue_conn:eval('queue.create_tube(...)', {tube_name, options})
    end, function(err)
        t.assert_str_contains(tostring(err), 'test_dlq_validation_already_exists_dlq could not be created')
    end)
end
