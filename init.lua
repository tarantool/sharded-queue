local vshard = require('vshard')
local log = require('log')

-- Bootstrap the vshard router.
while true do
    local ok, err = vshard.router.bootstrap({
        if_not_bootstrapped = true,
    })
    if ok then
        break
    end
    log.info(('Router bootstrap error: %s'):format(err))
end
