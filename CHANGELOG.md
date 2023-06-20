# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

## 0.1.0 - 2023-06-16

The first release provides cartridge roles that implement a distributed
message broker API, which is compatible with the
[Tarantool queue](https://github.com/tarantool/queue) module.

Please note that `sharded-queue` is not actually a queue, it implements a
message broker. The order of task identifiers is not guaranteed between
different shards (over the whole cluster).

### Added

- A Cartridge role for storages - `sharded_queue.storage`.
- A Cartridge role for api calls from a Cartridge application - `shareded_queue.api`.
- `fifo` API compatible driver with
  [fifor driver in queue module](https://github.com/tarantool/queue/#fifo---a-simple-queue).
- `fifottl` API compatible driver with
  [fifottl driver in queue module](https://github.com/tarantool/queue/#fifottl---a-simple-priority-queue-with-support-for-task-time-to-live).
- Metrics support for the api role (#55).
- `_VERSION` field for roles (#58).
- Testing CI (#53).
- Linter check on CI (#18).
- Publish CI (#54).
