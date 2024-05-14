local t = require('luatest')
local g = t.group('release_limit_test')

local fiber = require('fiber')

local helper = require('test.helper')
local utils = require('test.helper.utils')

g.before_all(function()
    g.queue_conn = helper.get_evaler('queue-router')
end)

for test_name, options in pairs({
    fifottl = {
        driver = 'sharded_queue.drivers.fifottl',
        release_limit = 5,
    },
    fifo = {
        driver = 'sharded_queue.drivers.fifo',
        release_limit = 5,
    }
}) do
    g['test_release_limit_' .. test_name] = function()
    local tube_name = 'test_release_limit_' .. test_name
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

    -- Release tasks several times to reach 'release_limit'
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

    -- All tasks should be deleted
    local stat = g.queue_conn:call('queue.statistics', { tube_name })

    t.assert_equals(stat.calls.delete, task_count)
    end
end

function g.test_ttr_release_limit_test()
    local tube_name = 'test_ttr_release_limit_test'
    helper.create_tube(
        tube_name,
        {
            driver = 'sharded_queue.drivers.fifottl',
            release_limit = 1,
        }
    )

    g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), {
        'simple data',
        { ttl = 60, ttr = 0.1 }
    })
    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'))
    t.assert_equals(task[utils.index.data], 'simple data')
    t.assert_equals(task[utils.index.status], utils.state.TAKEN)

    -- Wait for a ttr
    fiber.sleep(0.2)

    -- Task should be deleten because of release_limit = 1
    local stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(stat.calls.delete, 1)
end

function g.test_release_limit_validation()
    t.skip_if(utils.is_tarantool_3(), 'the role is available only for Cartridge')
    local tube_name = 'test_release_limit_validation'
    local options = {
        driver = 'sharded_queue.drivers.fifottl',
        release_limit = 'foo',
    }

    utils.try(function()
        g.queue_conn:eval('queue.create_tube(...)', {tube_name, options})
    end, function(err)
        t.assert_str_contains(tostring(err), 'release_limit must be number')
    end)
end
