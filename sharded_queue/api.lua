local cluster = require('cluster')
local net_box = require('net.box')
local vshard = require('vshard')
local fiber = require('fiber')
local log = require('log')

local time = require('sharded_queue.time')
local state = require('sharded_queue.state')
local utils = require('sharded_queue.utils')

local tube = require('sharded_queue.driver_fifottl')

local pool = require('cluster.pool')

local remote_call = function(method, instance_uri, args)
    local conn = pool.connect(instance_uri)
    return conn:call(method, { args })
end

local sharded_tube = {}

function sharded_tube.put(self, data, options)
    local bucket_count = vshard.router.bucket_count()
    local bucket_id = math.random(bucket_count)
    
    local options = options or {}
    
    options.data = data
    options.tube_name = self.tube_name
    options.bucket_id = bucket_id
    options.bucket_count = bucket_count

    local task = vshard.router.call(bucket_id,
        'write', 'tube_put', { options })
    return task
end

-- function for try get task from instance --   
function take_task(tube_name, storages)
    for _, instance_uri in pairs(storages) do
        -- try take task from all instance
        local ok, ret = pcall(remote_call, 'tube_take',
            instance_uri,
            {
                tube_name = tube_name
            })
        if ret ~= nil then
            return ret
        end
    end
end

function sharded_tube.take(self, timeout)
    -- take task from tube --

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        log.info(replica.master.uri)
        if utils.array_contains(replica.roles, 'sharded_queue.storage') then
            table.insert(storages, replica.master.uri)
        end
    end 
    utils.array_shuffle(storages)

    local task = take_task(self.tube_name, storages)
    
    if task ~= nil then
        return task
    end

    timeout = time.nano(timeout) or time.TIMEOUT_INFINITY

    local frequency = 1000
    local wait_part = 0.01 -- maximum waiting time in second

    local calc_part = time.sec(timeout / frequency) 
    
    if calc_part < wait_part then
        wait_part = tonumber(calc_part)
    end

    while timeout ~= 0 do
        local begin = time.cur()

        local cond = fiber.cond()
        cond:wait(wait_part)

        task = take_task(self.tube_name, storages)

        if task ~= nil then
            return task
        end

        local duration = time.cur() - begin
        timeout = timeout > duration and timeout - duration or 0
    end
end

function sharded_tube.delete(self, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_delete', {
        {
            tube_name = self.tube_name,
            task_id = task_id
        }
    })

    return task
end

function sharded_tube.release(self, task_id)
    -- task release from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_release', {
        {
            tube_name = self.tube_name,
            task_id = task_id    
        }
    })

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

    local task = vshard.router.call(bucket_id, 'write', 'tube_touch', {
        {
            tube_name = self.tube_name,
            task_id = task_id,
            delta = delta    
        }
    })
    return task
end

function sharded_tube.ask(self, task_id)
    -- task delete from tube --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_ask', {
        {
            tube_name = self.tube_name,
            task_id = task_id    
        }
    })

    return task
end

function sharded_tube.bury(self, task_id)
    -- task bury --

    local bucket_count = vshard.router.bucket_count()
    local bucket_id, _ = utils.unpack_task_id(task_id, bucket_count)

    local task = vshard.router.call(bucket_id, 'write', 'tube_bury', {
        {
            tube_name = self.tube_name,
            task_id = task_id    
        }
    })

    return task
end

function sharded_tube.kick(self, count)
    -- try kick few tasks --

    local storages = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        if utils.array_contains(replica.roles, 'sharded_queue.storage') then
            table.insert(storages, replica.master.uri)
        end
    end

    local kicked_count = 0 -- count kicked task
    for _, instance_uri in pairs(storages) do
        local ok, k = pcall(remote_call, 'tube_kick',
            instance_uri,
            {
                tube_name = self.tube_name,
                count     = count - kicked_count
            })
        if not ok then
            log.error(k)
            return
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

    local task = vshard.router.call(bucket_id, 'write', 'tube_peek', {
        {
            tube_name = self.tube_name,
            task_id = task_id    
        }
    })

    return task
end

function sharded_tube.drop(self)
    local tubes = cluster.config_get_deepcopy('tubes') or {}

    tubes[self.tube_name] = nil
    cluster.config_patch_clusterwide({ tubes = tubes })
end

local sharded_queue = {
    tube = {}
}

function sharded_queue.statistics(tube_name)
    if not tube_name then
        return
    end
    -- collect stats from all storages
    local stats_collection = {}
    for _, replica in pairs(cluster.admin.get_replicasets()) do
        if utils.array_contains(replica.roles, 'sharded_queue.storage') then
            --
            local ok, ret = pcall(remote_call, 'tube_statistic',
                replica.master.uri,
                {
                    tube_name = tube_name
                })
            --
            if not ok or ret == nil then
                return
            end
            --
            table.insert(stats_collection, ret)
        end
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
    local tubes = cluster.config_get_deepcopy('tubes') or {}

    if tubes[tube_name] ~= nil then
        -- already exist --
        return nil
    end

    tubes[tube_name] = options or {}
    cluster.config_patch_clusterwide({ tubes = tubes })

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
        for tube_name, _ in pairs(cfg_tubes) do
            if sharded_queue.tube[tube_name] == nil then
                local self = setmetatable({
                    tube_name = tube_name,
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
            end
        end
    end
end

return {
    init = init,
    apply_config = apply_config,
    dependencies = {
        'cluster.roles.vshard-router',
    }
}
