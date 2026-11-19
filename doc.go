// Package offsetkeeper is a Kafka consumer runtime on franz-go: at-least-once
// delivery, per-partition order, bounded retries and a confirmed dead-letter
// queue. It never commits an offset past a record that failed.
//
// The guarantees are in CONTRACT.md; each one names the test that proves it.
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
