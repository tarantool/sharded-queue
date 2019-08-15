local helper = {}

helper.state = {
    READY   = 'r',
    TAKEN   = 't',
    DONE    = '-',
    BURIED  = '!',
    DELAYED = '~',
}

helper.index = {
    task_id    = 1,
    bucket_id  = 2,
    status     = 3,
    created    = 4,
    priority   = 5,
    ttl        = 6,
    ttr        = 7,
    next_event = 8,
    data       = 9,
    index      = 10
}

function helper.sec(tm)
    if tm == nil then
        return
    end
    return tm / 1e6
end

function helper.cur()
    return 0ULL + fiber.time64()
end

function helper.nano(tm)
    if tm == nil then
        return
    end
    return 0ULL + tm * 1e6
end

function sign(val)
    return (val >= 0 and 1) or -1
end

function helper.round(val, bracket)
    bracket = bracket or 1
    return math.floor(val / bracket + sign(val) * 0.5) * bracket
end

function helper.array_contains(array, value, key)
    if not array then
        return false
    end
    local key = key or function(x) return x end
    for _, v in ipairs(array) do
        if key(v) == value then
            return true
        end
    end

    return false
end

-- a < b
function helper.subset_of(a, b)
    for _, value in pairs(a) do
        if not helper.array_contains(b, value) then
            return false
        end
    end
    return true
end

-- a = b
function helper.equal_sets(a, b)
    return helper.subset_of(a, b) and helper.subset_of(b, a)
end

return helper