#!/bin/bash -e

# simple topology

# - 1 router
# - 2 replicaset (storage and replica)

# init instances

mkdir -p ./dev

export HOSTNAME='localhost'

ALIAS='router'     BINARY_PORT=3301 HTTP_PORT=8081 CONSOLE_SOCK=/tmp/1.sock tarantool init.lua & echo $! >> ./dev/pids
ALIAS='s1-master'  BINARY_PORT=3302 HTTP_PORT=8082 CONSOLE_SOCK=/tmp/2.sock tarantool init.lua & echo $! >> ./dev/pids
ALIAS='s1-replica' BINARY_PORT=3303 HTTP_PORT=8083 CONSOLE_SOCK=/tmp/3.sock tarantool init.lua & echo $! >> ./dev/pids
ALIAS='s2-master'  BINARY_PORT=3304 HTTP_PORT=8084 CONSOLE_SOCK=/tmp/4.sock tarantool init.lua & echo $! >> ./dev/pids
ALIAS='s2-replica' BINARY_PORT=3305 HTTP_PORT=8085 CONSOLE_SOCk=/tmp/5.sock tarantool init.lua & echo $! >> ./dev/pids

sleep 2.5
echo "All instances started!"

# assemble cartridge

curl -w "\n" -X POST http://127.0.0.1:8081/admin/api --fail -d@- <<'QUERY'
{"query":
    "mutation {
        j1: join_server(
            uri:\"localhost:3301\",
            instance_uuid: \"aaaaaaaa-aaaa-4000-b000-000000000001\"
            replicaset_uuid: \"aaaaaaaa-0000-4000-b000-000000000000\"
        )
        j2: join_server(
            uri:\"localhost:3302\",
            instance_uuid: \"bbbbbbbb-bbbb-4000-b000-000000000001\"
            replicaset_uuid: \"bbbbbbbb-0000-4000-b000-000000000000\"
            timeout: 5
        )
        j3: join_server(
            uri:\"localhost:3303\",
            instance_uuid: \"bbbbbbbb-bbbb-4000-b000-000000000002\"
            replicaset_uuid: \"bbbbbbbb-0000-4000-b000-000000000000\"
            timeout: 5
        )
        j4: join_server(
            uri:\"localhost:3304\",
            instance_uuid: \"cccccccc-cccc-4000-b000-000000000001\"
            replicaset_uuid: \"cccccccc-0000-4000-b000-000000000000\"
            timeout: 5
        )
        j5: join_server(
            uri:\"localhost:3305\",
            instance_uuid: \"cccccccc-cccc-4000-b000-000000000002\"
            replicaset_uuid: \"cccccccc-0000-4000-b000-000000000000\"
            timeout: 5
        )
        e1: edit_replicaset(
            uuid: \"aaaaaaaa-0000-4000-b000-000000000000\"
            master: [\"aaaaaaaa-aaaa-4000-b000-000000000001\"]
            roles: [\"sharded_queue.api\"]
        )
        e2: edit_replicaset(
            uuid: \"bbbbbbbb-0000-4000-b000-000000000000\"
            master: [\"bbbbbbbb-bbbb-4000-b000-000000000001\"]
            roles: [\"sharded_queue.storage\"]
        )
        e3: edit_replicaset(
            uuid: \"cccccccc-0000-4000-b000-000000000000\"
            master: [\"cccccccc-cccc-4000-b000-000000000001\"]
            roles: [\"sharded_queue.storage\"]
        )
        bootstrap_vshard
    }"
}
QUERY

echo " - Cluster is ready!"