# This way everything works as expected ever for
# `make -C /path/to/project` or
# `make -f /path/to/project/Makefile`.
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
LUACOV_REPORT := $(PROJECT_DIR)/luacov.report.out

# Look for .rocks/bin directories upward starting from the project
# directory.
#
# It is useful for luacheck, luatest and LDoc.
#
# Note: The PROJECT_DIR holds a real path.
define ENABLE_ROCKS_BIN
	$(if $(wildcard $1/.rocks/bin),
		$(eval ROCKS_PATH := $(if $(ROCKS_PATH),$(ROCKS_PATH):,)$1/.rocks/bin)
	)
	$(if $1,
		$(eval $(call ENABLE_ROCKS_BIN,$(patsubst %/,%,$(dir $1))))
	)
endef
$(eval $(call ENABLE_ROCKS_BIN,$(PROJECT_DIR)))

# Add found .rocks/bin to PATH.
PATH := $(if $(ROCKS_PATH),$(ROCKS_PATH):$(PATH),$(PATH))

TTCTL := tt
ifeq (,$(shell which tt 2>/dev/null))
	TTCTL := tarantoolctl
endif

.PHONY: default
default:
	false

.PHONY: build
build:
	cartridge build

.PHONY: deps
deps:
	$(TTCTL) rocks install vshard 0.1.26
	$(TTCTL) rocks install luacheck 0.26.0
	$(TTCTL) rocks install luacov 0.13.0
	$(TTCTL) rocks install luacov-coveralls 0.2.3-1 --server=http://luarocks.org
	$(TTCTL) rocks install luatest 1.0.1

.PHONY: deps-cartridge
deps-cartridge: deps-cartridge
	$(TTCTL) rocks install cartridge 2.9.0

.PHONY: deps-metrics
deps-metrics: deps-metrics
	$(TTCTL) rocks install metrics 1.0.0

.PHONY: lint
lint:
	luacheck .

.PHONY: test
test:
	luatest -v --shuffle all

.PHONY: coverage
coverage:
	rm -f luacov*
	luatest -v --shuffle all --coverage
	sed "s|$(PROJECT_DIR)/||" -i luacov.stats.out
	luacov sharded_queue
	grep -A999 '^Summary' $(LUACOV_REPORT)
