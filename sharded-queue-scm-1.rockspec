package = 'sharded-queue'
version = 'scm-1'
source  = {
    url = 'git+ssh://git@gitlab.com:tarantool/sandbox/sharded-queue.git';
    branch = 'master';
}
dependencies = {
    'lua >= 5.1';
    'checks >= 3.0.0',
    'cluster >= 0.9.2',
}

external_dependencies = {
    TARANTOOL = {
        header = 'tarantool/module.h';
    };
}

build = {
    type = 'none'
}
