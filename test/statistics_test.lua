local helper = require('test.helper')
local queue = require('queue')
local fiber = require('fiber')

local t = require('luatest')

local g = t.group('statistics_test')

---

g.before_all = function()
    queue.init('localhost:3301')
end

g.after_all = function()
    queue.stop()
end

---

function g.test_statistics()
    local tube_name = 'statistics_test'
    local tube = queue.create_tube(tube_name)

    local task_count = 64
    local middle = 32

    local cur_stat = nil
    local task_pack = {}

    for i = 1, task_count do
        table.insert(task_pack, tube:put(i, { delay = 3 , ttl = 3, ttr = 1})[helper.index.task_id])
        
        if i == middle then -- it time to stop and check statistics
            cur_stat = queue.statistics(tube_name)

            t.assert_equals(cur_stat.tasks.delayed, middle)
            t.assert_equals(cur_stat.calls.put, middle)
        end
    end
    -- check all putten task
    cur_stat = queue.statistics(tube_name)

    t.assert_equals(cur_stat.tasks.delayed, task_count)
    t.assert_equals(cur_stat.calls.put, task_count)

    fiber.sleep(3.01)
    
    -- after delay
    cur_stat = queue.statistics(tube_name)
    t.assert_equals(cur_stat.tasks.delayed, 0)
    t.assert_equals(cur_stat.tasks.ready, task_count)

    -- take few task
    local taken_task_count = 20
    local taken_task_pack  = {}
    for i = 1, taken_task_count do
        table.insert(taken_task_pack, tube:take(0.001)[helper.index.task_id])
    end

    -- after taken
    cur_stat = queue.statistics(tube_name)
    t.assert_equals(cur_stat.tasks.ready, task_count - taken_task_count)
    t.assert_equals(cur_stat.tasks.taken, taken_task_count)
    t.assert_equals(cur_stat.calls.take, taken_task_count)

    -- done few task with ask
    local done_task_count = 10
    for i = 1, done_task_count do
        tube:ask(taken_task_pack[i])
    end

    -- after ask
    cur_stat = queue.statistics(tube_name)
    t.assert_equals(cur_stat.tasks.taken, taken_task_count - done_task_count)
    t.assert_equals(cur_stat.tasks.done, done_task_count)
    t.assert_equals(cur_stat.calls.ask, done_task_count)

    -- sleep time to run and check stats after ttr
    fiber.sleep(1.01)
    cur_stat = queue.statistics(tube_name)
    t.assert_equals(cur_stat.tasks.taken, 0)
    t.assert_equals(cur_stat.tasks.ready, task_count - done_task_count)

    -- sleep and wait auto remove by time to live
    fiber.sleep(3.01)
    cur_stat = queue.statistics(tube_name)
    t.assert_equals(cur_stat.tasks.taken, 0)
    t.assert_equals(cur_stat.tasks.ready, 0)
    t.assert_equals(cur_stat.tasks.done, task_count)

end