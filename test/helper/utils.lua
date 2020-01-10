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

return utils
