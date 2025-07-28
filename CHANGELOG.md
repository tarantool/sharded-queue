# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Method truncate() to clean tubes (#78).

### Changed

### Fixed

## 1.0.0 - 2024-04-17

The release introduces roles for Tarantool 3 and improves the module metrics.

### Added

- Metric `tnt_sharded_queue_router_role_stats` is a [summary][metrics-summary]
  with quantiles of `sharded_queue.api` role API calls (#71).
  The metric includes a counter of API calls and errors.
  The metric contains labels in the following format:
  `{name = "tube_name", method = "api_call_method", status = "ok" or "error"}`
- Metric `tnt_sharded_queue_storage_role_stats` is a [summary][metrics-summary]
  with quantiles of `sharded_queue.storage` role API calls (#71).
  The metric includes a counter of API calls and errors.
  The metric contains labels in the following format:
  `{name = "tube_name", method = "api_call_method", status = "ok" or "error"}`
- Metric `tnt_sharded_queue_storage_statistics_calls_total` as an equivalent of
  `tnt_sharded_queue_api_statistics_calls_total` for the
  `sharded_queue.storage` role (#71).
  Values have the same meaning as the [`queue` statistics][queue-statistics]
  `calls` table.
  The metric contains labels in the following format:
  `{name = "tube_name", state = "call_type"}`
- Metric `tnt_sharded_queue_storage_statistics_tasks` as an equivalent of
  `tnt_sharded_queue_api_statistics_tasks` for the `sharded_queue.storage`
  role (#71).
  Values have the same meaning as the [`queue` statistics][queue-statistics]
  `tasks` table.
  The metric contains labels in the following format:
  `{name = "tube_name", state = "task_state"}`
- Role `roles.sharded-queue-router` for Tarantool 3 (#68).
- Role `roles.sharded-queue-storage` for Tarantool 3 (#68).

### Changed

- Metric `sharded_queue_calls` renamed to
  `tnt_sharded_queue_router_statistics_calls_total` (#71). The metric now has
  labels in the format `{name = "tube_name", state = "call_type"}` instead of
  `{name = "tube_name", status = "call_type"}`.
- Metric `sharded_queue_tasks` renamed to
  `tnt_sharded_queue_router_statistics_tasks` (#71). The metric now has labels
  in the format `{name = "tube_name", state = "task_state"}` instead of
  `{name = "tube_name", status = "task_state"}`.
- The dependency `cartridge` is removed from the `rockspec` since the module
  does not require it to work with Tarantool 3 (#68).

### Fixed

- Data race with fifo driver for put()/take() methods with vinyl
  engine (#64).

## 0.1.1 - 2023-09-06

The release fixes the loss of tasks in the `fifottl` driver.

### Fixed

- A deletion of a released task after `ttr` in the `fifottl` driver (#65).

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

[metrics-summary]: https://www.tarantool.io/en/doc/latest/book/monitoring/api_reference/#summary
[queue-statistics]: https://github.com/tarantool/queue?tab=readme-ov-file#getting-statistics
