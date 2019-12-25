#! /bin/bash

.PHONY: all doc
all: doc
	mkdir -p doc

.PHONY: build
build:
	tarantoolctl rocks make sharded-queue-scm-1.rockspec

.PHONY: lint
lint:
	.rocks/bin/luacheck .

.PHONY: test
test:
	.rocks/bin/luatest -v --shuffle all
