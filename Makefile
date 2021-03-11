#! /bin/bash

.PHONY: all doc
all: doc
	mkdir -p doc

.PHONY: bootstrap
bootstrap:
	tarantoolctl rocks install luacheck 0.25.0
	tarantoolctl rocks install luatest 0.5.0
	tarantoolctl rocks install luacov 0.13.0

.PHONY: build
build:
	tarantoolctl rocks make sharded-queue-scm-1.rockspec

.PHONY: lint
lint:
	.rocks/bin/luacheck .

.PHONY: test
test: lint
	rm -f luacov*
	.rocks/bin/luatest -v --coverage
	.rocks/bin/luacov . && grep -A999 '^Summary' luacov.report.out
