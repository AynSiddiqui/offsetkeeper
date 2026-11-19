package offsetkeeper

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher is a thin acknowledged producer on franz-go defaults (acks=all, idempotent).
// Opts that weaken acks (kgo.RequiredAcks(kgo.LeaderAck()), kgo.NoAck(), kgo.DisableIdempotentWrite)
// void the contract; they are documented, not prevented.
//
// A Publisher is safe for concurrent use by multiple goroutines, as the kgo.Client
// it wraps is.
type Publisher struct{ cl *kgo.Client }

// NewPublisher creates a Publisher with the given franz-go options.
func NewPublisher(opts ...kgo.Opt) (*Publisher, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("offsetkeeper: create publisher: %w", err)
	}
	return &Publisher{cl}, nil
}

// Publish returns only after every record is acked, or the first error.
// It sets Partition, Offset, Timestamp and related fields on rs, as kgo does, so
// pass a record you no longer need to read as it was.
func (p *Publisher) Publish(ctx context.Context, rs ...*kgo.Record) error {
	// ProduceSync returns after every record has a result; FirstErr is that
	// result, so a nil return means the broker acked all of them.
	if err := p.cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		return fmt.Errorf("offsetkeeper: publish: %w", err)
	}
	return nil
}

// Close releases the Publisher's resources.
func (p *Publisher) Close() { p.cl.Close() }
