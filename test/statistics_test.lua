local t = require('luatest')
local g = t.group('statistics_test')

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')


g.before_all = function()
    g.queue_conn = config.cluster:server('queue-router').net_box
end

local function shape_cmd(tube_name, cmd)
    return string.format('queue.tube.%s:%s', tube_name, cmd)
end

function g.test_statistics()
    local tube_name = 'statistics_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local task_count = 64
    local middle = 32

    local cur_stat = nil
    local task_pack = {}

    for i = 1, task_count do
        table.insert(task_pack,
            g.queue_conn:call(shape_cmd(tube_name, 'put'), {
            i, { delay = 3 , ttl = 3, ttr = 1}
        })[utils.index.task_id])
        
        if i == middle then -- it time to stop and check statistics
            cur_stat = g.queue_conn:call('queue.statistics', { tube_name })

            t.assert_equals(cur_stat.tasks.delayed, middle)
            t.assert_equals(cur_stat.calls.put, middle)
        end
    end
    -- check all putten task
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })

    t.assert_equals(cur_stat.tasks.delayed, task_count)
    t.assert_equals(cur_stat.calls.put, task_count)

    fiber.sleep(3.01)
    
    -- after delay
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.delayed, 0)
    t.assert_equals(cur_stat.tasks.ready, task_count)

    -- take few task
    local taken_task_count = 20
    local taken_task_pack  = {}
    for i = 1, taken_task_count do
        table.insert(taken_task_pack,
            g.queue_conn:call(
                shape_cmd(tube_name, 'take'),
                { 0.001 }
            )[utils.index.task_id])
    end

    -- after taken
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.ready, task_count - taken_task_count)
    t.assert_equals(cur_stat.tasks.taken, taken_task_count)
    t.assert_equals(cur_stat.calls.take, taken_task_count)

    -- done few task with ask
    local done_task_count = 10
    for i = 1, done_task_count do
        g.queue_conn:call(shape_cmd(tube_name, 'ask'), { taken_task_pack[i] })
    end

    -- after ask
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.taken, taken_task_count - done_task_count)
    t.assert_equals(cur_stat.tasks.done, done_task_count)
    t.assert_equals(cur_stat.calls.ask, done_task_count)

    -- sleep time to run and check stats after ttr
    fiber.sleep(1.01)
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.taken, 0)
    t.assert_equals(cur_stat.tasks.ready, task_count - done_task_count)

    -- sleep and wait auto remove by time to live
    fiber.sleep(3.01)
    cur_stat = g.queue_conn:call('queue.statistics', { tube_name })
    t.assert_equals(cur_stat.tasks.taken, 0)
    t.assert_equals(cur_stat.tasks.ready, 0)
    t.assert_equals(cur_stat.tasks.done, task_count)

end