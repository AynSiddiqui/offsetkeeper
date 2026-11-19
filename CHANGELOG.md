# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/); versioning: [SemVer](https://semver.org/).

## Unreleased

- `Run`: Kafka consumer runtime on franz-go with at-least-once delivery, in-order handling per partition, bounded retries with capped backoff, and a dead-letter queue whose write is acked before the source offset is committed. Never commits past an unsettled record.
- `Permanent` to skip retries; handler panics and `runtime.Goexit` are treated as failures.
- `Publisher`: acknowledged producer on franz-go defaults (acks=all, idempotent).
- `offsetkeeper dlq list` and `offsetkeeper dlq replay` CLI.
- `CONTRACT.md` with a named test for every guarantee; contract tests run on kfake, Apache Kafka and Redpanda.
