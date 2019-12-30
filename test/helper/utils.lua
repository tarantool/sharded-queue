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

local function sign(val)
    return (val >= 0 and 1) or -1
end

function utils.round(val, bracket)
    bracket = bracket or 1
    return math.floor(val / bracket + sign(val) * 0.5) * bracket
end

function utils.array_contains(array, value, key)
    if not array then
        return false
    end
    key = key or function(x) return x end
    for _, v in ipairs(array) do
        if key(v) == value then
            return true
        end
    end

    return false
end

-- a < b
function utils.subset_of(a, b)
    for _, value in pairs(a) do
        if not utils.array_contains(b, value) then
            return false
        end
    end
    return true
end

-- a = b
function utils.equal_sets(a, b)
    return utils.subset_of(a, b) and utils.subset_of(b, a)
end

return utils
