local helper = require('test.helper')
local queue = require('queue')
local fiber = require('fiber')

local t = require('luatest')

local g = t.group('timeout_test')

function math.sign(val)
    return (val >= 0 and 1) or -1
end

function math.round(val, bracket)
    bracket = bracket or 1
    return math.floor(val / bracket + math.sign(val) * 0.5) * bracket
end

--

g.before_all = function()
    queue.init('localhost:3301')
end

g.after_all = function()
    queue.stop()
end


local function task_take(tube, timeout, channel)
    -- fiber function for take task with timeout and calc duration time
    local start = fiber.time64()
    local task = tube:take(timeout)
    local duration = fiber.time64() - start
    
    channel:put(duration)
    channel:put(task)

    fiber.kill(fiber.id())
end
--

function g.test_try_waiting()
    -- TAKE task with timeout
    -- CHECK uptime and value - nil

    local tube_name = 'try_waiting_test'
    local tube = queue.create_tube(tube_name)
    local timeout = 3 -- second

    local channel = fiber.channel(2)
    local task_fiber = fiber.create(task_take, tube, timeout, channel)

    fiber.sleep(timeout)

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_equals(math.round(waiting_time, 0.1), 3)
    t.assert_equals(task, nil)

    channel:close()
end
--

function g.test_wait_put_taking()
    -- TAKE task with timeout
    -- WAIT timeout value / 2
    -- PUT task to tube
    -- CHEK what was taken successfully

    local tube_name = 'wait_put_taking_test'
    local tube = queue.create_tube(tube_name)
    local timeout = 3

    local channel = fiber.channel(2)
    local task_fiber = fiber.create(task_take, tube, timeout, channel)

    fiber.sleep(timeout / 2)
    tube:put('simple_task')

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_equals(math.round(waiting_time, 0.1), timeout / 2)
    t.assert_equals(task[8], 'simple_task')

    channel:close()
end