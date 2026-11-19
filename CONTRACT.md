# Contract

What offsetkeeper promises, and the test that proves each promise. A promise
without a passing test is not a promise. Tests run on an in-process kfake
cluster, Apache Kafka and Redpanda.

## Guarantees

1. **At-least-once.** A handler may see a record more than once (crash,
   rebalance, failed commit). A record whose handler returned nil is never
   lost. Proof: `TestCrashBeforeCommitRedelivers`, `TestKafkaKeyIsNotMessageID`.
2. **Order within a partition.** Records of one partition are handled one at a
   time in offset order; the next starts only after the previous is settled,
   including across split fetches of the same partition.
   Partitions run concurrently; there is no order across partitions.
   Proof: `TestPartitionOrder`, `TestBatchSplitFetchSamePartition` (internal).
3. **Settlement.** A handler returning nil settles the record. An error is
   retried for at most `MaxAttempts` total attempts with doubling backoff,
   unless wrapped in `Permanent`; a recovered panic counts as an error. After the
   last attempt, with `DLQTopic` the DLQ record is produced and its ack settles
   the source. Without `DLQTopic`, or if the DLQ produce fails, `Run` returns an
   error naming topic, partition and offset, and commits nothing at or past it.
   A DLQ produce that keeps failing with a retriable broker error is cut off
   after 30s and counts as failed. A handler goroutine that ends without
   returning (`runtime.Goexit`, for example `t.FailNow`) is a failure of that
   record too: `Run` returns an error and the partition's earlier settled
   records stay committed.
   Proof: `TestNoCommitPastFailedRecord`, `TestFailedDLQPublishDoesNotSettleSource`,
   `TestPermanentSkipsRetries`, `TestHandlerPanicIsAnError`,
   `TestHandlerReturnOwnsSettlement`, `TestHandlerGoexitStopsRun`,
   `TestDLQProduceTimeoutStopsRun` (kfake only).
4. **Commit.** Offsets are committed synchronously after each poll batch, at the
   highest contiguous settled offset per partition (offset plus one). Never past an
   unsettled record. When `Run` stops on an error, the settled prefix of every
   partition is committed first, not only the failing one's. A commit error makes
   `Run` return it. Handlers cannot change the committed offset by modifying
   their record.
   Proof: `TestNoCommitPastFailedRecord`, `TestHandlerReturnOwnsSettlement`,
   `TestErrorExitCommitsOtherPartitions`, `TestBatchErrorCommitsOtherPartitions` (internal),
   `TestCommitErrorStopsRun` (kfake only), `TestHandlerCannotMoveCommit`.
5. **Rebalance.** A rebalance waits until the current batch is settled and
   committed. If a rebalance begins while a batch is in flight, the consumer
   is evicted only if the remaining batch work exceeds the rebalance timeout.
   Proof: `TestRebalanceDuringHandler`.
6. **Shutdown.** On context cancel, cancellation is observed between records:
   no new records or retries are started. Records whose handler already returned
   nil or settled via DLQ are committed, then `Run` returns nil (or any commit
   error if committing fails). Unsettled records are redelivered later. A
   handler that fails only because of the cancel is not dead-lettered.
   Proof: `TestShutdownDrains`, `TestShutdownDrainsWithDLQ`,
   `TestNoRecordHandledAfterCancel`, `TestRetryBackoffStopsOnShutdown`.
7. **DLQ record.** Same key, value and headers as the source, plus
   `offsetkeeper.source.topic`, `offsetkeeper.source.partition`,
   `offsetkeeper.source.offset`, `offsetkeeper.error` (capped at 4 KiB) and
   `offsetkeeper.attempts`. Mutating topic or offset in the handler does not
   alter these headers. The DLQ write is confirmed before the source offset is
   committed, and a replayed record is processed once more. On replay, records
   are produced to their original source topic with the partition chosen by the
   broker partitioner, and all headers prefixed with `offsetkeeper.` are
   reserved and stripped before replaying. Replay is subject to topic retention
   (expired records cannot be replayed) and consumer group offset expiry (an
   expired `offsetkeeper-replay.<topic>` group offset may duplicate a retained
   queue). `ReplayOne` is independent of `ReplayAll`'s tracking (group
   `offsetkeeper-replay.<topic>`): it does not move that group's offset, so a
   later `ReplayAll` replays the same record again if it is still uncheckpointed
   and retained. If a DLQ topic contains an invisible terminal marker (e.g. from
   transactional writers), list or replay waits for a visible record or
   cancellation (Ctrl-C). Proof: `TestDLQWriteConfirmedBeforeSourceCommit`,
   `TestReplayedRecordIsProcessedOnceMore`, `TestDLQNamesOriginalSource`,
   `TestDLQErrorHeaderCapped`.
8. **Publish.** `Publisher.Publish` returns nil only after the broker has
   acked every record with acks=all and idempotence enabled. The same durability
   guarantee applies to the DLQ produce under `Run` when using default
   `Config.ClientOpts`. Weakening options passed to `NewPublisher` (for
   publishing) or `Config.ClientOpts` (for DLQ produce) void this guarantee.
   Proof: `TestProduceIsAcknowledged`, `TestPublishUsesAcksAllAndIdempotence` (kfake only).

## Not promised

- **Exactly-once side effects.** Make handlers idempotent.
- **Order across partitions.**
- **Protection when `ClientOpts` weaken acks** (for example `kgo.RequiredAcks`
  below all, or disabling idempotence). Such options void guarantees 3, 7 and 8.
- **Handler batches longer than the rebalance timeout.** A poll batch,
  retries included, must finish within the group rebalance timeout (kgo
  default 60s) once a rebalance starts, or the group may evict the consumer and
  redeliver the batch. Heartbeats continue in the background while processing, so
  a long batch causes eviction only if a rebalance begins mid-batch and outlives
  the rebalance timeout. A batch is up to 500 records per poll, handled in order
  per partition. The worst-case retry time of one record is the sum of its
  backoffs (doubling from `Backoff`, capped at 30s each) and must fit in that
  batch time budget. For example `MaxAttempts=5, Backoff=10s` waits
  10+20+30+30 = 90s for one failing record; if a rebalance begins early in that
  window, the member is evicted.
- **A bounded wait on a DLQ produce the broker never answers.** The 30s DLQ
  timeout turns a produce that fails with a broker error into a `Run` error.
  If the request was already sent and no response ever arrives, kgo cannot
  fail the record safely (idempotent produce), so `Run` waits until kgo fails
  it, for example when the connection drops.
