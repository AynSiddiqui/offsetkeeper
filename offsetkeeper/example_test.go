package offsetkeeper_test

import (
	"context"
	"errors"
	"log"

	"github.com/AynSiddiqui/offsetkeeper/offsetkeeper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ExampleRun() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := offsetkeeper.Run(ctx, offsetkeeper.Config{
		Brokers:     []string{"localhost:9092"},
		Group:       "billing",
		Topics:      []string{"invoices"},
		MaxAttempts: 3,
		DLQTopic:    "invoices.dlq",
		Handler: func(_ context.Context, r *kgo.Record) error {
			log.Printf("handled %s", r.Value)
			return nil // nil settles the record; an error retries, then dead-letters
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}

func ExamplePermanent() {
	handler := func(_ context.Context, r *kgo.Record) error {
		if len(r.Value) == 0 {
			// Retrying cannot fix an empty payload: skip the retries, go to the DLQ.
			return offsetkeeper.Permanent(errors.New("empty payload"))
		}
		return nil
	}
	_ = offsetkeeper.Config{Handler: handler}
}

func ExamplePublisher_Publish() {
	p, err := offsetkeeper.NewPublisher(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()
	// Publish returns only after the broker has acked every record.
	if err := p.Publish(context.Background(), &kgo.Record{Topic: "invoices", Value: []byte("hello")}); err != nil {
		log.Fatal(err)
	}
}
