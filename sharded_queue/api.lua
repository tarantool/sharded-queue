local cartridge = require('cartridge')
local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')

local time = require('sharded_queue.time')
local utils = require('sharded_queue.utils')

local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local remote_call = function(method, instance_uri, args, timeout)
    local conn = cartridge_pool.connect(instance_uri)
    return conn:call(method, { args }, { timeout = timeout })
end

local map = function(method, args, uri_list)
    local fibers_data = {}

    for _, uri in ipairs(uri_list) do
        local fiber_obj = fiber.new(
            remote_call, method, uri, args)

        fiber_obj:name('netbox_map')
        fiber_obj:set_joinable(true)
        fibers_data[uri] = fiber_obj
    end

    local map_data = {}

    for _, fiber_obj in pairs(fibers_data) do
        local ok, ret = fiber_obj:join()

        if not ok or ret == nil then
            return false, ret
        end

        table.insert(map_data, ret)
    end

    return true, map_data
end

local function validate_options(options)
    if not options then return true end

    if options.wait_factor then
        if type(options.wait_factor) ~= 'number'
            or options.wait_factor < 1
        then
            return false, "wait_factor must be number greater than or equal to 1"
        end
    end

    return true
end

local sharded_tube = {}

function sharded_tube.put(self, data, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id = math.random(bucket_count)

    options = options or {}

    options.data = data
    options.tube_name = self.tube_name
    options.bucket_id = bucket_id
    options.bucket_count = bucket_count

    local task, err = vshard.router.call(bucket_id,
        'write', 'tube_put', { options })
    -- re-raise storage errors
    if err ~= nil then error(err) end
    return task
end

-- function for try get task from instance --
local function take_task(storages, options, take_timeout, call_timeout)
    for _, instance_uri in ipairs(storages) do
        if take_timeout == 0 then
            break
        end
        local begin = time.cur()

        -- try take task from all instance
        local ok, ret = pcall(remote_call, 'tube_take',
            instance_uri,
            options,
            call_timeout
        )

        if ret ~= nil and ok then
            return ret
        end

        local duration = time.cur() - begin
        take_timeout = take_timeout > duration and take_timeout -  duration or 0
    end
end

function sharded_tube.take(self, timeout, options)
    -- take task from tube --
    if options == nil then
        options = {}
    end
    options.tube_name = self.tube_name

    local wait_factor = self.wait_factor

    local remote_call_timeout = time.MIN_NET_BOX_CALL_TIMEOUT
    if timeout ~= nil and timeout > time.MIN_NET_BOX_CALL_TIMEOUT then
        remote_call_timeout = timeout
    end

    local take_timeout = time.nano(timeout) or time.TIMEOUT_INFINITY

    local frequency = 1000
    local wait_part = 0.01 -- maximum waiting time in second

    local calc_part = time.sec(take_timeout / frequency)

    if calc_part < wait_part then
        wait_part = tonumber(calc_part)
    end

    while take_timeout ~= 0 do
        local begin = time.cur()

        local storages = cartridge_rpc.get_candidates(
            'sharded_queue.storage',
            { leader_only = true })

        utils.array_shuffle(storages)

        local task = take_task(storages, options, take_timeout, remote_call_timeout)

        if task ~= nil then return task end

        if take_timeout < time.nano(wait_part) then
            return nil
        else
            fiber.sleep(wait_part)
            wait_part = wait_part * wait_factor
        end

        local duration = time.cur() - begin

        take_timeout = take_timeout > duration and take_timeout - duration or 0
    end
end

function sharded_tube.delete(self, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_delete', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.release(self, task_id)
    -- task release from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_release', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.touch(self, task_id, delta)
    if delta == nil or delta <= 0 then
        return
    end

    if delta >= time.MAX_TIMEOUT then
        delta = time.TIMEOUT_INFINITY
    else
        delta = time.nano(delta)
    end

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_touch', {
        {
            tube_name = self.tube_name,
            task_id = task_id,
            delta = delta
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.ack(self, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_ack', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.bury(self, task_id)
    -- task bury --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_bury', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.kick(self, count)
    -- try kick few tasks --

    local storages = cartridge_rpc.get_candidates(
        'sharded_queue.storage',
        {
            leader_only = true
        })

    local kicked_count = 0 -- count kicked task
    for _, instance_uri in ipairs(storages) do
        local ok, k = pcall(remote_call, 'tube_kick',
            instance_uri,
            {
                tube_name = self.tube_name,
                count     = count - kicked_count
            })
        if not ok then
            log.error(k)
            return kicked_count
        end

        kicked_count = kicked_count + k

        if kicked_count == count then
            break
        end
    end

    return kicked_count
end

function sharded_tube.peek(self, task_id)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task, err = vshard.router.call(bucket_id, 'write', 'tube_peek', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })
    -- re-raise storage errors
    if err ~= nil then error(err) end

    return task
end

function sharded_tube.drop(self)
    local tubes = cartridge.config_get_deepcopy('tubes') or {}

    tubes[self.tube_name] = nil
    cartridge.config_patch_clusterwide({ tubes = tubes })
end

local sharded_queue = {
    tube = {}
}

function sharded_queue.statistics(tube_name)
    if not tube_name then
        return
    end
    -- collect stats from all storages
    local storages = cartridge_rpc.get_candidates(
        'sharded_queue.storage',
        {
            leader_only = true
        })

    local ok, stats_collection = map('tube_statistic',
        { tube_name = tube_name }, storages)

    if not ok or stats_collection == nil then
        log.info(stats_collection)
        return
    end
    -- merge
    local stat = stats_collection[1]
    for i = 2, #stats_collection do
        for name, count in pairs(stats_collection[i].tasks) do
            stat.tasks[name] = stat.tasks[name] + count
        end
        for name, count in pairs(stats_collection[i].calls) do
            stat.calls[name] = stat.calls[name] + count
        end
    end
    --
    return stat
end

function sharded_queue.create_tube(tube_name, options)
    local tubes = cartridge.config_get_deepcopy('tubes') or {}

    if tubes[tube_name] ~= nil then
        -- already exist --
        return nil
    end

    local ok , err = validate_options(options)
    if not ok then error(err) end

    tubes[tube_name] = options or {}
    ok, err = cartridge.config_patch_clusterwide({ tubes = tubes })
    if not ok then error(err) end

    return sharded_queue.tube[tube_name]
end

local function init(opts)
    if opts.is_master then
        rawset(_G, 'queue', sharded_queue)
    end
end

local function apply_config(cfg, opts)
    if opts.is_master then
        local cfg_tubes = cfg.tubes or {}

        -- try init tubes --
        for tube_name, options in pairs(cfg_tubes) do
            if sharded_queue.tube[tube_name] == nil then
                local self = setmetatable({
                    tube_name = tube_name,
                    wait_factor = options.wait_factor or time.DEFAULT_WAIT_FACTOR,
                }, {
                    __index = sharded_tube
                })
                sharded_queue.tube[tube_name] = self
            end
        end

        -- try drop tubes --
        for tube_name, _ in pairs(sharded_queue.tube) do
            if cfg_tubes[tube_name] == nil then
                setmetatable(sharded_queue.tube[tube_name], nil)
                sharded_queue.tube[tube_name] = nil
            end
        end
    end
end

-- FIXME: Remove when https://github.com/tarantool/cartridge/issues/308 resolved
local function queue_action_wrapper(action)
    return function(name, ...)
        if not sharded_queue.tube[name] then
            return nil, string.format('No queue "%s" initialized yet', name)
        end

        return sharded_queue.tube[name][action](sharded_queue.tube[name], ...)
    end
end

return {
    init = init,
    apply_config = apply_config,
    put = queue_action_wrapper('put'),
    take = queue_action_wrapper('take'),
    delete = queue_action_wrapper('delete'),
    release = queue_action_wrapper('release'),
    touch = queue_action_wrapper('touch'),
    ack = queue_action_wrapper('ack'),
    bury = queue_action_wrapper('bury'),
    kick = queue_action_wrapper('kick'),
    peek = queue_action_wrapper('peek'),
    drop = queue_action_wrapper('drop'),
    statistics = sharded_queue.statistics,

    dependencies = {
        'cartridge.roles.vshard-router',
    },

    __private = {
        sharded_tube = sharded_tube,
    }
}
