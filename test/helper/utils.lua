local fiber = require('fiber')

local utils = {}

utils.state = {
    READY   = 'r',
    TAKEN   = 't',
    DONE    = '-',
    BURIED  = '!',
    DELAYED = '~',
}

utils.index = {
    task_id    = 1,
    status     = 2,
    data       = 3
}

function utils.sec(tm)
    if tm == nil then
        return
    end
    return tm / 1e6
end

function utils.cur()
    return 0ULL + fiber.time64()
end

function utils.nano(tm)
    if tm == nil then
        return
    end
    return 0ULL + tm * 1e6
end

function utils.shape_cmd(tube_name, cmd)
    return string.format('queue.tube.%s:%s', tube_name, cmd)
end

function utils.is_metrics_supported()
    local is_package, metrics = pcall(require, "metrics")
    if not is_package then
        return false
    end
    -- metrics >= 0.11.0 is required
    local counter = require('metrics.collectors.counter')
    return metrics.unregister_callback and counter.remove and true or false
end

return utils
