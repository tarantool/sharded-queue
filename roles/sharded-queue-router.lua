local config = require('config')

local router_config = require('sharded_queue.router.config')
local is_metrics_supported = require('sharded_queue.metrics').is_supported
local metrics = require('sharded_queue.router.metrics')
local roles = require('sharded_queue.roles')
local utils = require('sharded_queue.utils')
local queue = require('sharded_queue.router.queue')

local role_name = roles.get_tarantool_role_name('router')
local storage_role_name = roles.get_tarantool_role_name('storage')

local function collect_tubes()
    local tubes = nil
    for instance in pairs(config:instances()) do
        local roles = config:get('roles', {instance = instance}) or {}
        local found = false
        for _, role in ipairs(roles) do
            if role == storage_role_name then
                found = true
                break
            end
        end
        if found then
            local roles_cfg = config:get('roles_cfg', {instance = instance}) or {}
            local storage_cfg = roles_cfg[storage_role_name] or {}
            if tubes == nil then
                tubes = storage_cfg.tubes
            elseif not table.equals(tubes, storage_cfg.tubes) then
                error(role_name .. ": tubes configurations on storages are not equal", 2)
            end
        end
    end
    return tubes
end

local function validate(conf)
    if not roles.is_tarantool_role_supported() then
        error(role_name .. ": the role is supported only for Tarantool 3.0.2 or newer")
    end
    if not roles.is_sharding_role_enabled('router') then
        error(role_name .. ": instance must be a sharding router to use the role")
    end

    conf = conf or {}
    local tubes = collect_tubes() or {}
    local ok, err = utils.validate_tubes(tubes, false)
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

    queue.export_globals()

    local conf_tubes = collect_tubes() or {}
    local conf_cfg = conf.cfg or {}
    if conf_cfg.metrics ~= nil then
        router_config.metrics = conf_cfg.metrics and true or false
    else
        router_config.metrics = is_metrics_supported()
    end

    for tube_name, options in pairs(conf_tubes) do
        if queue.map()[tube_name] == nil then
            queue.add(tube_name, metrics, options)
        end
    end

    for tube_name, _ in pairs(queue.map()) do
        if conf_tubes[tube_name] == nil then
            queue.remove(tube_name)
        end
    end

    if router_config.metrics then
        metrics.enable(queue)
    else
        metrics.disable()
    end
end

local function stop()
    queue.clear_globals()
end

return {
    validate = validate,
    apply = apply,
    stop = stop,
}
