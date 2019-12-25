package = 'sharded-queue'
version = 'scm-1'
source  = {
    url = 'git+https://github.com/tarantool/sharded-queue.git';
    branch = 'master';
}
dependencies = {
    'lua >= 5.1';
    'checks >= 3.0.0',
    'cartridge >= 1.2.0, < 3.0.0',
}

external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h';
    };
}

build = {
    type = 'make',
	build_target = 'all',
    install = {
        lua = {
            ['sharded_queue.api'] = 'sharded_queue/api.lua',
            ['sharded_queue.storage'] = 'sharded_queue/storage.lua',
            ['sharded_queue.driver_fifottl'] = 'sharded_queue/driver_fifottl.lua',
            ['sharded_queue.time'] = 'sharded_queue/time.lua',
            ['sharded_queue.utils'] = 'sharded_queue/utils.lua',
            ['sharded_queue.state'] = 'sharded_queue/state.lua',
        },
    },
    build_variables = {
        version = 'scm-1',
    },
    install_pass = false,
}
