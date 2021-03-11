#!/usr/bin/env bash

echo "building..."
make build

echo "bootstrapping..."
make bootstrap

echo "Running lint"
make lint

echo "Running tests"
.rocks/bin/luatest -v --coverage
.rocks/bin/luacov . && grep -A999 '^Summary' luacov.report.out
