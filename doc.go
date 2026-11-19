// Package offsetkeeper is a Kafka consumer runtime on franz-go: at-least-once
// delivery, per-partition order, bounded retries and a confirmed dead-letter
// queue. It never commits an offset past a record that is unsettled: one that
// has failed, was skipped, or is still being handled.
//
// The guarantees are in CONTRACT.md; each one names the test that proves it.
//
// # How a batch is committed
//
// Run polls up to 500 records and groups them by partition. One goroutine per
// partition hands its records to the Handler in offset order and stops at the
// first record that is not settled (handled without error, or acked by the
// DLQ). When every goroutine is done, Run commits the last record of each
// partition's contiguous settled prefix, then calls AllowRebalance and polls
// again. On cancel no new record is started, the settled prefix is committed
// and Run returns nil (or the commit error). If a record cannot be settled (no DLQ, or the DLQ write
// failed), the other partitions are cancelled, their settled prefixes are
// committed, and Run returns the error.
//
// Example:
//
//	err := offsetkeeper.Run(ctx, offsetkeeper.Config{
//		Brokers:  []string{"localhost:9092"},
//		Group:    "billing",
//		Topics:   []string{"invoices"},
//		DLQTopic: "invoices.dlq",
//		Handler: func(ctx context.Context, r *kgo.Record) error {
//			return process(ctx, r.Value) // nil settles; an error retries, then dead-letters
//		},
//	})
package offsetkeeper
