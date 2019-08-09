local t = require('luatest')
local g = t.group('simple_test')

package.path = package.path .. ";../?.lua"

local queue = require('queue')
local utils = require('utils')

local state = {
    READY   = 'r',
    TAKEN   = 't',
    DONE    = '-',
    BURIED  = '!',
    DELAYED = '~',
}

---

g.before_all = function()
    queue.init('localhost:3301')
end

g.after_all = function()
    queue.stop()
end

---

function g.test_put_taken()
    local tube_name = 'put_taken_test'
    local tube = queue.create_tube(tube_name)

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
        local task = tube:put(data, {})
        -- print(task[1])
        table.insert(task_ids, task[1])
    end
    -- try taken this tasks
    local taken_task_ids = {}
    for i = 1, #task_ids do
        local task = tube:take()
        t.assert_equals(task[3], state.TAKEN)
        table.insert(taken_task_ids, task[1])
    end
    -- compare
    t.assert_equals(utils.equal_sets(task_ids, taken_task_ids), true)
end

function g.test_delete()
    local tube_name = 'delete_test'
    local tube = queue.create_tube(tube_name)

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
        local task = tube:put(data, {})
        table.insert(task_ids, task[1])
    end

    -- delete few tasks
    local deleted_tasks_count = 10
    local deleted_tasks = {}

    utils.array_shuffle(task_ids)

    for i = 1, deleted_tasks_count do
        table.insert(deleted_tasks, tube:delete(task_ids[i])[1])
    end

    -- taken tasks
    local taken_task_ids = {}
    for i = 1, task_count - deleted_tasks_count do
        local task = tube:take()
        t.assert_equals(task[3], state.TAKEN)
        table.insert(taken_task_ids, task[1])
    end
    --
    local excepted_task_ids = {}
    for i = deleted_tasks_count + 1, #task_ids do
        table.insert(excepted_task_ids, task_ids[i])
    end

    -- compare
    t.assert_equals(utils.equal_sets(excepted_task_ids, taken_task_ids), true)
end

