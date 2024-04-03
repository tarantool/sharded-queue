local utils = require('test.helper.utils')

if utils.is_tarantool_3() then
    return require('test.helper.config_tarantool')
else
    return require('test.helper.config_cartridge')
end
