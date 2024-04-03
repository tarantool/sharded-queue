local fio = require('fio')
local t = require('luatest')
local fun = require('fun')
local yaml = require('yaml')

local server = require('test.helper.server')

local roles = {
    ['router'] = 'roles.sharded-queue-router',
    ['storage'] = 'roles.sharded-queue-storage',
}

local config = {}

config.root = fio.dirname(fio.abspath(package.search('init')))
config.datadir = fio.pathjoin(config.root, 'dev')
config.configpath = fio.pathjoin(config.root, 'test', 'entrypoint', 'config.yml')
config.devconfigpath = fio.pathjoin(config.root, 'dev', 'config.yml')

function config.start_example_replicaset()
    local opts = {
        config_file = config.devconfigpath,
        chdir = config.datadir,
    }
    local servers = {
        "queue-router",
        "queue-router-1",
        "queue-storage-1-0",
        "queue-storage-1-1",
        "queue-storage-2-0",
        "queue-storage-2-1",
    }
    config.servers = {}
    for _, server_name in ipairs(servers) do
        local server_opts = fun.chain(opts, {alias = server_name}):tomap()
        config.servers[server_name] = server:new(server_opts)
    end

    for _, srv in pairs(config.servers) do
        srv:start({wait_until_ready = false})
    end

    for _, srv in pairs(config.servers) do
        srv:wait_until_ready()
    end
    config.bootstrap()
end

function config.stop_example_replicaset()
    for _, srv in pairs(config.servers) do
        srv:drop()
    end
end

function config.bootstrap()
    local routers = {
        config.servers['queue-router'],
        config.servers['queue-router-1'],
    }
    for _, router in ipairs(routers) do
        local _, err = router:eval("require('vshard').router.bootstrap({if_not_bootstrapped = true})")
        t.assert_not(err)
    end
end

function config.reload()
    for _, srv in pairs(config.servers) do
        srv:eval("require('config'):reload()")
    end
    for _, srv in pairs(config.servers) do
        srv:wait_until_ready()
    end
    config.bootstrap()
end

local function read_config(path)
    local src = fio.open(path)
    local data = src:read()
    src:close()
    return yaml.decode(data)
end

local function write_config(path, decoded)
    local dst = fio.open(path, {'O_CREAT', 'O_WRONLY', 'O_TRUNC'})
    dst:write(yaml.encode(decoded))
    dst:close()
end

function config.get_cfg()
    local decoded = read_config(config.devconfigpath)

    if decoded['roles_cfg'] ~= nil and decoded['roles_cfg'][roles[1]] then
        return decoded['roles_cfg'][roles[1]]['cfg']
    end
    return nil
end

function config.set_cfg(cfg)
    local decoded = read_config(config.devconfigpath)

    if decoded['roles_cfg'] == nil then
        decoded['roles_cfg'] = {}
    end
    for _, role in pairs(roles) do
        if decoded['roles_cfg'][role] == nil then
            decoded['roles_cfg'][role] = {}
        end
        decoded['roles_cfg'][role]['cfg'] = cfg
    end

    write_config(config.devconfigpath, decoded)

    config.reload()
end

function config.create_tube(tube_name, options)
    options = options or {}

    local decoded = read_config(config.devconfigpath)

    if decoded['roles_cfg'] == nil then
        decoded['roles_cfg'] = {}
    end

    local role = roles['storage']
    if decoded['roles_cfg'][role] == nil then
        decoded['roles_cfg'][role] = {}
    end
    if decoded['roles_cfg'][role]['tubes'] == nil then
        decoded['roles_cfg'][role]['tubes'] = {}
    end
    decoded['roles_cfg'][role]['tubes'][tube_name] = {}
    for k, v in pairs(options or {}) do
        decoded['roles_cfg'][role]['tubes'][tube_name][k] = v
    end

    write_config(config.devconfigpath, decoded)

    config.reload()
end

function config.drop_tube(tube_name)
    local decoded = read_config(config.devconfigpath)

    if decoded['roles_cfg'] == nil then
        return
    end

    local role = roles['storage']
    if decoded['roles_cfg'][role] == nil then
        return
    end
    if decoded['roles_cfg'][role]['tubes'] == nil then
        return
    end
    if decoded['roles_cfg'][role]['tubes'][tube_name] == nil then
        return
    end
    decoded['roles_cfg'][role]['tubes'][tube_name] = nil

    write_config(config.devconfigpath, decoded)

    config.reload()
end

function config.eval(server_name, ...)
    return config.servers[server_name]:eval(...)
end

function config.get_evaler(server_name)
    return config.servers[server_name]
end

t.before_suite(function()
    fio.rmtree(config.datadir)
    fio.mktree(config.datadir)

    fio.copyfile(config.configpath, config.devconfigpath)
    fio.copytree(fio.pathjoin(config.root, 'roles'),
        fio.pathjoin(config.datadir, 'roles'))
    fio.copytree(fio.pathjoin(config.root, 'sharded_queue'),
        fio.pathjoin(config.datadir, 'sharded_queue'))

    config.start_example_replicaset()
end)

t.after_suite(function()
    config.stop_example_replicaset()
end)

return config
