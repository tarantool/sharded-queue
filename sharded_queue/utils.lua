local fiber = require('fiber')

local metrics = require('sharded_queue.metrics')

local utils = {}

local function atomic_tail(status, ...)
    if not status then
        box.rollback()
        error((...), 2)
     end
     box.commit()
     return ...
end

-- box.atomic(opts, fun, args) does not supported for all Tarantool's versions,
-- so we an analog.
function utils.atomic(fun, ...)
    if box.cfg.memtx_use_mvcc_engine then
        -- max() + insert() or min() + update() do not work as expected with
        -- best-effort visibility: for write transactions it chooses
        -- read-committed, for read transactions it chooses read-confirmed.
        --
        -- So max()/min() could return the same tuple even if a concurrent
        -- insert()/update() has been committed, but has not confirmed yet.
        box.begin({txn_isolation = 'read-committed'})
    else
        box.begin()
    end
    return atomic_tail(pcall(fun, ...))
end

function utils.array_shuffle(array)
    if not array then return nil end
    math.randomseed(tonumber(0ULL + fiber.time64()))

    for i = #array, 1, -1 do
        local j = math.random(i)
        array[i], array[j] = array[j], array[i]
    end
end

--[[
task_id = internal_index * (bucket_count + 1) + bucket_id
Increment is necessary to avoid the case when the bucket_id id is equal bucket_count
To get bucket_id and internal_index you need to perform the reverse operation
--]]

function utils.pack_task_id(bucket, bucket_count, index)
    return index * (bucket_count + 1) + bucket
end

function utils.unpack_task_id(task_id, bucket_count)
    local index = math.floor(task_id / (bucket_count + 1))
    local bucket = task_id - index * (bucket_count + 1)
    return bucket, index
end

utils.normalize = {}

function utils.normalize.log_request(log_request)
    if log_request and type(log_request) ~= 'boolean' then
        return false, "log_request must be boolean"
    end
    return log_request or false
end

function utils.normalize.wait_max(wait_max)
    if wait_max ~= nil then
        if type(wait_max) ~= 'number' or wait_max <= 0 then
            return nil, "wait_max must be number greater than 0"
        end
    end
    return wait_max
end

function utils.validate_tubes(tubes, on_storage)
    for tube_name, tube_opts in pairs(tubes) do
        if tube_opts.driver ~= nil then
            if type(tube_opts.driver) ~= 'string' then
                local msg = 'Driver name must be a valid module name for tube %s'
                return nil, msg:format(tube_name)
            end
            if on_storage then
                local ok, _ = pcall(require, tube_opts.driver)
                if not ok then
                    local msg = 'Driver %s could not be loaded for tube %s'
                    return nil, msg:format(tube_opts.driver, tube_name)
                end
            end
        end
    end

    return true
end

function utils.validate_cfg(cfg)
    if cfg == nil then
        return true
    end

    if type(cfg) ~= 'table' then
        return nil, '"cfg" must be a table'
    end
    if cfg.metrics and type(cfg.metrics) ~= 'boolean' then
        return nil, '"cfg.metrics" must be a boolean'
    end
    if cfg.metrics and cfg.metrics == true then
        if not metrics.is_supported() then
            return nil, "metrics >= 0.11.0 is required"
        end
    end

    return true
end

return utils
