local config = require('sharded_queue.storage.config')
local is_metrics_supported = require('sharded_queue.metrics').is_supported
local methods = require('sharded_queue.storage.methods')
local metrics = require('sharded_queue.storage.metrics')
local roles = require('sharded_queue.roles')
local stats_storage = require('sharded_queue.stats.storage')
local tubes = require('sharded_queue.storage.tubes').new()
local utils = require('sharded_queue.utils')

local role_name = roles.get_tarantool_role_name('storage')
local watcher = nil

local function validate(conf)
    if not roles.is_tarantool_role_supported() then
        error(role_name .. ": the role is supported only for Tarantool 3.0.2 or newer")
    end
    if not roles.is_sharding_role_enabled('storage') then
        error(role_name .. ": instance must be a sharding storage to use the role")
    end

    conf = conf or {}
    local ok, err = utils.validate_tubes(conf.tubes or {}, true)
    if not ok then
        error(role_name .. ": " .. err)
    end
    ok, err = utils.validate_cfg(conf.cfg or {})
    if not ok then
        error(role_name .. ": " .. err)
    end
    return true
end

local function apply(conf)
    conf = conf or {}

    local conf_tubes = conf.tubes or {}
    local conf_cfg = conf.cfg or {}
    if conf_cfg.metrics ~= nil then
        config.metrics = conf_cfg.metrics and true or false
    else
        config.metrics = is_metrics_supported()
    end

    if watcher ~= nil then
        watcher:unregister()
    end
    watcher = box.watch('box.status', function(_, status)
        if status.is_ro == false then
            stats_storage.init()

            local new = tubes:update(conf_tubes)
            for _, tube in ipairs(new) do
                stats_storage.reset(tube)
            end

            if config.metrics then
                metrics.enable(tubes:map())
            end
            methods.init(metrics, tubes)
        end
    end)

    if config.metrics then
        metrics.enable(tubes:map())
    else
        metrics.disable()
    end
end

local function stop()
    if watcher ~= nil then
        watcher:unregister()
        watcher = nil
    end
end

return {
    validate = validate,
    apply = apply,
    stop = stop,
}
