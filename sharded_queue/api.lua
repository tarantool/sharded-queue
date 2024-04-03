local cartridge = require('cartridge')

local config = require('sharded_queue.router.config')
local metrics = require('sharded_queue.router.metrics')
local utils = require('sharded_queue.utils')
local queue = require('sharded_queue.router.queue')

function cfg_call(_, options)
    options = options or {}
    if options.metrics == nil then
        return
    end

    if type(options.metrics) ~= 'boolean' then
        error('"metrics" must be a boolean')
    end

    if config.metrics ~= options.metrics then
        local tubes = cartridge.config_get_deepcopy('tubes') or {}

        if tubes['cfg'] ~= nil and tubes['cfg'].metrics == nil then
            error('tube "cfg" exist, unable to update a default configuration')
        end

        tubes['cfg'] = {metrics = options.metrics}
        local ok, err = cartridge.config_patch_clusterwide({ tubes = tubes })
        if not ok then error(err) end
    end
end

local function init(opts)
    queue.export_globals()
end

local function validate_config(cfg)
    local cfg_tubes = cfg.tubes or {}
    local ok, err = utils.validate_tubes(cfg_tubes, false)
    if not ok then
        return ok, err
    end
    return utils.validate_cfg(cfg_tubes['cfg'])
end

local function apply_config(cfg, opts)
    local cfg_tubes = cfg.tubes or {}
    -- try init tubes --
    for tube_name, options in pairs(cfg_tubes) do
        if tube_name == 'cfg' then
            if options.metrics ~= nil then
                config.metrics = options.metrics and true or false
            end
        elseif queue.map()[tube_name] == nil then
            queue.add(tube_name, metrics, options)
        end
    end

    -- try drop tubes --
    for tube_name, _ in pairs(queue.map()) do
        if tube_name ~= 'cfg' and cfg_tubes[tube_name] == nil then
            queue.remove(tube_name)
        end
    end

    if config.metrics then
        metrics.enable(queue)
    else
        metrics.disable()
    end
end

local function queue_action_wrapper(action)
    return function(name, ...)
        return queue.call(name, action, ...)
    end
end

return {
    init = init,
    apply_config = apply_config,
    validate_config = validate_config,

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

    cfg = setmetatable({}, {
        __index = config,
        __newindex = function() error("Use api.cfg() instead", 2) end,
        __call = cfg_call,
        __serialize = function() return config end,
    }),
    statistics = queue.statistics,
    _VERSION = require('sharded_queue.version'),

    dependencies = {
        'cartridge.roles.vshard-router',
    },
}
