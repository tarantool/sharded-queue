local t = require('luatest')
local g = t.group('exponential_backoff_test')

local log = require('log') -- luacheck: ignore

local config = require('test.helper.config')
local utils = require('test.helper.utils')
local fiber = require('fiber')

g.before_all(function()
    --- Workaround for https://github.com/tarantool/cartridge/issues/462
    config.cluster:server('queue-router').net_box:close()
    config.cluster:server('queue-router').net_box = nil
    config.cluster:server('queue-router'):connect_net_box()
    g.queue_conn = config.cluster:server('queue-router').net_box
end)

local function task_take(tube_name, timeout, channel)
    -- fiber function for take task with timeout and calc duration time
    local start = fiber.time64()
    local task = g.queue_conn:call(utils.shape_cmd(tube_name, 'take'), { timeout })
    local duration = fiber.time64() - start

    channel:put(duration)
    channel:put(task)
end
--

function g.test_default_timeout()
    local tube_name = 'test_default_wait_factor'
    g.queue_conn:call('queue.create_tube', {
        tube_name
    })

    local timeout = 3 -- second
    local attemts = 7 -- attempts count
    local put_wait = 0.01*(math.pow(2, attemts) - 1)   -- 1.27
    local take_time_expected = 0.01*(math.pow(2, attemts+1) - 1) -- 2.55

    local channel = fiber.channel(2)
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(put_wait + 0.2)

    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=0.5}))

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, take_time_expected, 0.1)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end
--

function g.test_success()
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.05
    -- 0.06 : take fail => wait 0.25
    -- 0.31 : take fail => wait 1.25
    -- 0.35 : put task
    -- 1.56 : take success
    -- expected time is 1.56 in case wait_factor = 5

    local tube_name = 'test_success_exp_backoff'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 5,
        }
    })

    local timeout = 7

    local channel = fiber.channel(2)
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(1)
    t.assert(g.queue_conn:call(utils.shape_cmd(tube_name, 'put'), { 'simple_task' }, {timeout=1}))

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 1.56, 0.1)
    t.assert_equals(task[utils.index.data], 'simple_task')

    channel:close()
end

function g.test_timeout()
    -- start taking
    -- 0.00 : take fail => wait 0.01 (see wait_part in api.lua)
    -- 0.01 : take fail => wait 0.05
    -- 0.06 : take fail => wait 0.25
    -- 0.31 : take fail => wait 1.25
    -- 1.56 : take fail => wait 6.25 (next try shoud be after 7.81)
    -- 7.00 : timeout


    local tube_name = 'test_timeout_exp_backoff'
    g.queue_conn:call('queue.create_tube', {
        tube_name,
        {
            wait_factor = 5,
        }
    })

    local timeout = 7

    local channel = fiber.channel(2)
    fiber.create(task_take, tube_name, timeout, channel)

    fiber.sleep(timeout)

    local waiting_time = tonumber(channel:get()) / 1e6
    local task = channel:get()

    t.assert_almost_equals(waiting_time, 1.56, 0.1)
    -- t.assert_almost_equals(waiting_time, 7, 0.1)
    t.assert_equals(task, nil)

    channel:close()
end
