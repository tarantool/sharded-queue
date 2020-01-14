local t = require('luatest')
local g = t.group('timeout_test')

local log = require('log') -- luacheck: ignore

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')

g.before_all = function()
    --- Workaround for https://github.com/tarantool/cartridge/issues/462
    config.cluster:server('queue-router').net_box:close()
    config.cluster:server('queue-router').net_box = nil
    config.cluster:server('queue-router'):connect_net_box()
    g.queue_conn = config.cluster:server('queue-router').net_box
end

local function shape_cmd(tube_name, cmd)
    return string.format('queue.tube.%s:%s', tube_name, cmd)
end

local function task_take(tube_name, timeout, channel)
    -- fiber function for take task with timeout and calc duration time
    local start = fiber.time64()
    local task = g.queue_conn:call(shape_cmd(tube_name, 'take'), { timeout })
    local duration = fiber.time64() - start

    channel:put(duration)
    channel:put(task)
end
--

function g.test_try_waiting()
    -- TAKE task with timeout
    -- CHECK uptime and value - nil

    local tube_name = 'try_waiting_test'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local timeout = 3 -- second

    local channel = fiber.channel(2)
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(timeout)

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 3, 0.1)
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
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local timeout = 3

    local channel = fiber.channel(2)
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(timeout / 2)
    t.assert(g.queue_conn:call(shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=1}))

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, timeout / 2, 0.1)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end
