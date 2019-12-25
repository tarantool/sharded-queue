#!/bin/bash

TAG=$(git describe --exact-match HEAD 2>/dev/null)
if [ -z "$TAG" ]
then
	echo "Skipping release: no git tag found."
	exit 0
fi

echo TAG = \"$TAG\"
mkdir -p release
sed -e "s/branch = '.\+'/tag = '$TAG'/g" \
    -e "s/version = 'scm-1'/version = '$TAG-1'/g" \
    sharded-queue-scm-1.rockspec > release/sharded-queue-$TAG-1.rockspec

tarantoolctl rocks make release/sharded-queue-$TAG-1.rockspec
tarantoolctl rocks pack sharded-queue $TAG && mv sharded-queue-$TAG-1.all.rock release/
