local config = require('sharded_queue.storage.config')
local methods = require('sharded_queue.storage.methods')
local metrics = require('sharded_queue.storage.metrics')
local stats_storage = require('sharded_queue.stats.storage')
local tubes = require('sharded_queue.storage.tubes').new()
local utils = require('sharded_queue.utils')

local function init(opts)

end

local function validate_config(cfg)
    local cfg_tubes = cfg.tubes or {}

    local ok, err = utils.validate_tubes(cfg_tubes, true)
    if not ok then
        return ok, err
    end
    return utils.validate_cfg(cfg_tubes['cfg'])
end

local function apply_config(cfg, opts)
    local cfg_tubes = table.deepcopy(cfg.tubes or {})
    if cfg_tubes['cfg'] ~= nil then
        local options = cfg_tubes['cfg']
        if options.metrics ~= nil then
            config.metrics = options.metrics and true or false
        end
        cfg_tubes['cfg'] = nil
    end

    if opts.is_master then
        stats_storage.init()

        local new = tubes:update(cfg_tubes)
        for _, tube in ipairs(new) do
            stats_storage.reset(tube)
        end

        methods.init(metrics, tubes)
    end

    if config.metrics then
        metrics.enable(tubes:map())
    else
        metrics.disable()
    end

    return true
end

return {
    init = init,
    validate_config = validate_config,
    apply_config = apply_config,
    _VERSION = require('sharded_queue.version'),

    dependencies = {
        'cartridge.roles.vshard-storage',
    },
}
