# Contract

What offsetkeeper promises, and the test that proves each promise. A promise
without a passing test is not a promise. Tests run on an in-process kfake
cluster, Apache Kafka and Redpanda.

## Guarantees

1. **At-least-once.** A handler may see a record more than once (crash,
   rebalance, failed commit). A record whose handler returned nil is never
   lost. Proof: `TestCrashBeforeCommitRedelivers`, `TestKafkaKeyIsNotMessageID`.
2. **Order within a partition.** Records of one partition are handled one at a
   time in offset order; the next starts only after the previous is settled.
   Partitions run concurrently; there is no order across partitions.
   Proof: `TestPartitionOrder`.
3. **Settlement.** A handler returning nil settles the record. An error is
   retried up to `MaxAttempts` with doubling backoff, unless wrapped in
   `Permanent`; a recovered panic counts as an error. After the last attempt,
   with `DLQTopic` the DLQ record is produced and its ack settles the source.
   Without `DLQTopic`, or if the DLQ produce fails, `Run` returns an error
   naming topic, partition and offset, and commits nothing at or past it.
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
   highest contiguous settled offset per partition. Never past an unsettled
   record. When `Run` stops on an error, the settled prefix of every partition
   is committed first, not only the failing one's. A commit error makes `Run`
   return it.
   Proof: `TestNoCommitPastFailedRecord`, `TestHandlerReturnOwnsSettlement`,
   `TestErrorExitCommitsOtherPartitions`, `TestCommitErrorStopsRun`.
5. **Rebalance.** A rebalance waits until the current batch is settled and
   committed. Proof: `TestRebalanceDuringHandler`.
6. **Shutdown.** On context cancel there are no new polls, records or retries.
   Records whose handler already returned nil are committed, then `Run`
   returns nil. Unsettled records are redelivered later.
   A handler that fails only because of the cancel is not dead-lettered.
   Proof: `TestShutdownDrains`, `TestShutdownDrainsWithDLQ`,
   `TestNoRecordHandledAfterCancel`, `TestRetryBackoffStopsOnShutdown`.
7. **DLQ record.** Same key, value and headers as the source, plus
   `offsetkeeper.source.topic`, `.source.partition`, `.source.offset`,
   `offsetkeeper.error` and `offsetkeeper.attempts`. The DLQ write is
   confirmed before the source offset is committed, and a replayed record is
   processed once more. `ReplayOne` is independent of `ReplayAll`'s tracking
   (group `offsetkeeper-replay.<topic>`): it does not move that group's offset,
   so a later `ReplayAll` replays the same record again. Proof: `TestDLQWriteConfirmedBeforeSourceCommit`,
   `TestReplayedRecordIsProcessedOnceMore`.
8. **Publish.** `Publisher.Publish` returns nil only after the broker has
   acked every record. Proof: `TestProduceIsAcknowledged`.

## Not promised

- **Exactly-once side effects.** Make handlers idempotent.
- **Order across partitions.**
- **Protection when `ClientOpts` weaken acks** (for example `kgo.RequiredAcks`
  below all, or disabling idempotence). Such options void guarantees 3, 7 and 8.
- **Handler batches longer than the rebalance timeout.** A poll batch,
  retries included, must finish within the group rebalance timeout (kgo
  default 60s), or the group may evict the consumer and redeliver the batch.
  A batch is up to 500 records per poll, handled in order per partition. The
  worst-case retry time of one record is the sum of its backoffs (doubling from
  `Backoff`, capped at 30s each) and must fit in that budget. For example
  `MaxAttempts=5, Backoff=10s` waits 10+20+30+30 = 90s for one failing record,
  so that member is always evicted.
- **A bounded wait on a DLQ produce the broker never answers.** The 30s DLQ
  timeout turns a produce that fails with a broker error into a `Run` error.
  If the request was already sent and no response ever arrives, kgo cannot
  fail the record safely (idempotent produce), so `Run` waits until kgo fails
  it, for example when the connection drops.
