package offsetkeeper

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
)

// dlqTimeout bounds one DLQ produce. kgo retries a retriable produce error (for
// example NOT_ENOUGH_REPLICAS) forever, which would hold the poll gate past the
// rebalance timeout with nothing returned. A var only so tests can shorten it.
var dlqTimeout = 30 * time.Second

// deadLetter produces src to Config.DLQTopic on the consumer's own client and
// returns only after the broker acked it. The ack is what settles src: Run
// commits past src only after this returns nil.
func (w *worker) deadLetter(ctx context.Context, src *kgo.Record, attempts int, herr error) error {
	rec := &kgo.Record{
		Topic:   w.cfg.DLQTopic,
		Key:     src.Key,
		Value:   src.Value,
		Headers: dlq.Headers(src, attempts, herr),
	}
	// On timeout kgo fails the record as soon as that is safe, and settle sees the
	// parent ctx still live, so the failure reaches Run as an error. If the request
	// was already sent and the broker never answers, kgo cannot fail it safely and
	// this waits (docs/CONTRACT.md "Not promised").
	pctx, cancel := context.WithTimeout(ctx, dlqTimeout)
	defer cancel()
	if err := w.cl.ProduceSync(pctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("offsetkeeper: dead-letter %s/%d offset %d to %q: %w", src.Topic, src.Partition, src.Offset, w.cfg.DLQTopic, err)
	}
	return nil
}
