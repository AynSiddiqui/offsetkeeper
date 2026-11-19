# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/); versioning: [SemVer](https://semver.org/).

## Unreleased

### Added

- `Run`: Kafka consumer runtime on franz-go with at-least-once delivery, in-order handling per partition, bounded retries with capped backoff, and a dead-letter queue whose write is acked before the source offset is committed. Never commits past an unsettled record.
- `Permanent` error wrapper to skip retries; handler panics and `runtime.Goexit` are treated as failures.
- `Publisher`: acknowledged producer on franz-go defaults (acks=all, idempotent).
- `offsetkeeper dlq list` and `offsetkeeper dlq replay` CLI tools.
- CLI `-tls` flag to connect using TLS with system root certificates (no SASL support).
- Streaming row output in `offsetkeeper dlq list` as records arrive from the broker.
- `CONTRACT.md` defining guarantees with named proofs across kfake, Apache Kafka, and Redpanda.
- Contract and internal tests: `TestHandlerCannotMoveCommit`, `TestDLQNamesOriginalSource`, `TestPublishUsesAcksAllAndIdempotence` (kfake-only), `TestDLQErrorHeaderCapped`, and `TestBatchSplitFetchSamePartition` (internal).

### Fixed

- Handlers receive a per-attempt shallow copy of `kgo.Record` so in-handler modifications or re-produces cannot move committed offsets or overwrite original DLQ source headers (`TestHandlerCannotMoveCommit`, `TestDLQNamesOriginalSource`). Key, Value, and Headers backing bytes are shared and must not be mutated in place.
- Cap DLQ `offsetkeeper.error` header at 4 KiB with a truncation marker to prevent oversized error text from failing broker produce and permanently halting consumption (`TestDLQErrorHeaderCapped`).
- Emit a `Warn` log (`offsetkeeper: dead-lettered`) on each dead-letter write with topic, partition, offset, DLQ topic, and attempt count.
- Reject configurations where `DLQTopic` is included in `Topics` to prevent recursive dead-letter loops.
- Bound CLI `-partition` flag to `math.MaxInt32` before client creation to avoid integer overflow and unintended partition 0 operations.
- Escape tabs, carriage returns, newlines, and backslashes in free-text columns of `dlq list` output.
- Fixed rebalance and shutdown documentation to clarify that eviction occurs only if a rebalance begins mid-batch and exceeds the rejoin timeout, and `Run` returns commit errors on shutdown.
