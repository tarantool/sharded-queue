local helper = {}

helper.state = {
    READY   = 'r',
    TAKEN   = 't',
    DONE    = '-',
    BURIED  = '!',
    DELAYED = '~',
}

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