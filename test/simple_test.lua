local helper = require('test.helper')
local queue = require('queue')

local t = require('luatest')

local g = t.group('simple_test')

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
        table.insert(task_ids, task[helper.index.task_id])
    end
    -- try taken this tasks
    local taken_task_ids = {}
    for i = 1, #task_ids do
        local task = tube:take()
        t.assert_equals(task[helper.index.status], helper.state.TAKEN)
        table.insert(taken_task_ids, task[helper.index.task_id])
    end
    -- compare
    t.assert_equals(helper.equal_sets(task_ids, taken_task_ids), true)
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
        table.insert(task_ids, task[helper.index.task_id])
    end

    -- delete few tasks
    local deleted_tasks_count = 10
    local deleted_tasks = {}

    for i = 1, deleted_tasks_count do
        table.insert(deleted_tasks, tube:delete(task_ids[i])[helper.index.task_id])
    end

    -- taken tasks
    local taken_task_ids = {}
    for i = 1, task_count - deleted_tasks_count do
        local task = tube:take()
        t.assert_equals(task[helper.index.status], helper.state.TAKEN)
        table.insert(taken_task_ids, task[helper.index.task_id])
    end
    --
    local excepted_task_ids = {}
    for i = deleted_tasks_count + 1, #task_ids do
        table.insert(excepted_task_ids, task_ids[i])
    end

    -- compare
    t.assert_equals(helper.equal_sets(excepted_task_ids, taken_task_ids), true)
end

function g.test_release()
    local tube_name = 'release_test'
    local tube = queue.create_tube(tube_name)

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
        local task = tube:put(data, {})
        t.assert_equals(task[helper.index.status], helper.state.READY)
        table.insert(task_ids, task[helper.index.task_id])
    end

    -- take few tasks
    local taken_task_count = 5
    local taken_task_ids = {}

    for i = 1, taken_task_count do
        local task = tube:take()
        t.assert_equals(task[helper.index.status], helper.state.TAKEN)
        table.insert(taken_task_ids, task[helper.index.task_id])
    end

    t.assert_equals(helper.subset_of(taken_task_ids, task_ids), true)

    for _, task_id in pairs(taken_task_ids) do
        local task = tube:release(task_id)
        t.assert_equals(task[helper.index.status], helper.state.READY)
    end

    local result_task_id = {}

    for i = 1, task_count do
        table.insert(result_task_id, tube:take()[helper.index.task_id])
    end

    t.assert_equals(helper.equal_sets(task_ids, result_task_id), true)
end