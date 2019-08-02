#!/bin/bash -e

# You can do it manually in web ui at http://localhost:8081/

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
            roles: [\"api\"]
        )
        e2: edit_replicaset(
            uuid: \"bbbbbbbb-0000-4000-b000-000000000000\"
            master: [\"bbbbbbbb-bbbb-4000-b000-000000000001\"]
            roles: [\"storage\"]
        )
        e3: edit_replicaset(
            uuid: \"cccccccc-0000-4000-b000-000000000000\"
            master: [\"cccccccc-cccc-4000-b000-000000000001\"]
            roles: [\"storage\"]
        )
        bootstrap_vshard
    }"
}
QUERY
echo " - Cluster is ready!"
