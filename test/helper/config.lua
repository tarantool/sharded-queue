local fio = require('fio')
local t = require('luatest')
local cluster_helpers = require('cluster.test_helpers')
require('json').cfg { encode_use_tostring = true }

local config = {}

config.root = fio.dirname(fio.abspath(package.search('init')))
config.datadir = fio.pathjoin(config.root, 'dev')

config.cluster = cluster_helpers.Cluster:new({
    datadir = config.datadir,
    server_command = fio.pathjoin(config.root, 'init.lua'),
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
                    advertise_port = 3301,
                    net_box_credentials = {
                        user = 'cluster',
                        password = 'sharded-queue-cookie'
                    },
                    env = {
                        ['ALIAS'] = 'queue-router',
                        ['BINARY_PORT'] = '3301',
                        ['HTTP_PORT'] = '8081',
                        ['HOSTNAME'] = 'localhost'
                    }
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
                    alias = 'queue-storage-1',
                    advertise_port = 3302,
                    net_box_credentials = {
                        user = 'cluster',
                        password = 'sharded-queue-cookie'
                    },
                    env = {
                        ['ALIAS'] = 'queue-storage-1',
                        ['BINARY_PORT'] = '3302',
                        ['HTTP_PORT'] = '8082',
                        ['HOSTNAME'] = 'localhost'
                    }
                },
            }
        },
        {
            uuid = 'cccccccc-0000-4000-b000-000000000000',
            roles = {'sharded_queue.storage'},
            servers = {
                {
                    instance_uuid = 'cccccccc-cccc-4000-b000-000000000001',
                    alias = 'queue-storage-2',
                    advertise_port = 3303,
                    net_box_credentials = {
                        user = 'cluster',
                        password = 'sharded-queue-cookie'
                    },
                    env = {
                        ['ALIAS'] = 'queue-storage-2',
                        ['BINARY_PORT'] = '3303',
                        ['HTTP_PORT'] = '8083',
                        ['HOSTNAME'] = 'localhost'
                    }
                }
            },
        }
    }
})

t.before_suite(function ()
    fio.rmtree(config.datadir)
    fio.mktree(config.datadir)
    config.cluster:start() end)

t.after_suite(function () config.cluster:stop() end)

return config
