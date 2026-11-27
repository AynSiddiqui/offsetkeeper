# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning: [SemVer](https://semver.org/).

## [Unreleased]

## [v0.2.1] - 2026-11-28

### Fixed

- `Run` no longer stalls 50-70s at shutdown when the broker is unreachable: the group leave is bounded at 5s, so `Run` returns about 17s after the cancel (10s commit + 5s leave + kgo's close).

## [v0.2.0] - 2026-11-28

Pre-1.0 breaking release: the import path changed.

### Changed

- **BREAKING:** the library moved from the module root to `offsetkeeper/`. Change `import "github.com/AynSiddiqui/offsetkeeper"` to `import "github.com/AynSiddiqui/offsetkeeper/offsetkeeper"` and run `go get github.com/AynSiddiqui/offsetkeeper/offsetkeeper`; `offsetkeeper.X` calls are unchanged. The CLI install path is unchanged.
- `CONTRACT.md`, `CONTRIBUTING.md`, `STYLE.md`, `SECURITY.md` and `CODE_OF_CONDUCT.md` moved into `docs/`; update links to them.

## [v0.1.0] - 2026-11-20

First release.

### Added

- `Run`: a Kafka consumer runtime on franz-go with at-least-once delivery, in-order handling per partition, bounded retries with capped backoff, and a dead-letter queue whose write is acked before the source offset is committed. It never commits past an unsettled record.
- `Config` validation: required fields, no empty broker or topic names, and no `DLQTopic` that is also consumed.
- `Permanent` to skip retries. Handler panics and `runtime.Goexit` count as failures. Each handler attempt gets its own copy of the record, so a handler cannot move the committed offset.
- DLQ records keep the source key, value and headers and add `offsetkeeper.*` headers; the error text is capped at 4 KiB, and each dead-letter is logged at `Warn`.
- `Publisher`: an acknowledged producer on franz-go defaults (acks=all, idempotent) that rejects nil records.
- `offsetkeeper dlq list` (streams rows, escapes free text) and `offsetkeeper dlq replay` (one record, or every record not yet replayed), with `-tls` and a 15s connection check.
- `CONTRACT.md`, with a named test for every guarantee. Contract and edge-case suites run on kfake, Apache Kafka and Redpanda.

[Unreleased]: https://github.com/AynSiddiqui/offsetkeeper/compare/v0.2.1...HEAD
[v0.2.1]: https://github.com/AynSiddiqui/offsetkeeper/compare/v0.2.0...v0.2.1
[v0.2.0]: https://github.com/AynSiddiqui/offsetkeeper/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/AynSiddiqui/offsetkeeper/releases/tag/v0.1.0
