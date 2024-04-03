local fio = require('fio')
local t = require('luatest')
local cartridge_helpers = require('cartridge.test-helpers')
local utils = require('test.helper.utils')

require('json').cfg { encode_use_tostring = true }

local config = {}

config.root = fio.dirname(fio.abspath(package.search('init')))

config.datadir = fio.pathjoin(config.root, 'dev')

config.cluster = cartridge_helpers.Cluster:new({
    datadir = config.datadir,
    server_command = fio.pathjoin(config.root, 'test', 'entrypoint', 'init.lua'),
    use_vshard = true,
    replicasets = {
        {
            uuid = 'aaaaaaaa-0000-4000-b000-000000000000',
            roles = {
                'sharded_queue.api'
            },
            servers = {
                {
                    instance_uuid = 'aaaaaaaa-aaaa-4000-b000-000000000001',
                    alias = 'queue-router',
                    advertise_port = 3311,
                    http_port = 9081,
                    cluster_cookie = 'sharded-queue-cookie',
                },
                {
                    instance_uuid = 'aaaaaaaa-aaaa-4000-b000-000000000002',
                    alias = 'queue-router-1',
                    advertise_port = 3312,
                    http_port = 9082,
                    cluster_cookie = 'sharded-queue-cookie',
                }
            },
        },
        {
            uuid = 'bbbbbbbb-0000-4000-b000-000000000000',
            roles = {
                'sharded_queue.storage'
            },
            servers = {
                {
                    instance_uuid = 'bbbbbbbb-bbbb-4000-b000-000000000001',
                    alias = 'queue-storage-1-0',
                    advertise_port = 3313,
                    http_port = 9083,
                    cluster_cookie = 'sharded-queue-cookie',
                },
                {
                    instance_uuid = 'bbbbbbbb-bbbb-4000-b000-000000000002',
                    alias = 'queue-storage-1-1',
                    advertise_port = 3314,
                    http_port = 9084,
                    cluster_cookie = 'sharded-queue-cookie',
                },
            }
        },
        {
            uuid = 'cccccccc-0000-4000-b000-000000000000',
            roles = {'sharded_queue.storage'},
            servers = {
                {
                    instance_uuid = 'cccccccc-cccc-4000-b000-000000000001',
                    alias = 'queue-storage-2-0',
                    advertise_port = 3315,
                    http_port = 9085,
                    cluster_cookie = 'sharded-queue-cookie',
                },
                {
                    instance_uuid = 'cccccccc-cccc-4000-b000-000000000002',
                    alias = 'queue-storage-2-1',
                    advertise_port = 3316,
                    http_port = 9086,
                    cluster_cookie = 'sharded-queue-cookie',
                },
            },
        }
    }
})

function config.get_cfg()
    return config.eval('queue-router', "return require('sharded_queue.api').cfg")
end

function config.set_cfg(cfg)
    config.eval('queue-router', "require('sharded_queue.api').cfg(...)",
        {cfg})
end

function config.create_tube(tube_name, options)
    config.eval('queue-router', "queue.create_tube(...)", {tube_name, options})
end

function config.drop_tube(tube_name)
    pcall(function()
        config.eval('queue-router', utils.shape_cmd(tube_name, 'drop') .. "()")
    end)
end

function config.eval(server, ...)
    return config.cluster:server(server).net_box:eval(...)
end

function config.get_evaler(server)
    return config.cluster:server(server).net_box
end

t.before_suite(function()
    fio.rmtree(config.datadir)
    fio.mktree(config.datadir)
    config.cluster:start()
    config.servers = {}
    for _, srv in pairs(config.cluster.servers) do
        config.servers[srv.alias] = srv
    end
end)

t.after_suite(function()
    config.cluster:stop()
end)

return config
