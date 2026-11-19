package offsetkeeper

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Publisher is a thin acknowledged producer on franz-go defaults (acks=all, idempotent).
// Opts that weaken acks void the contract; they are documented, not prevented.
type Publisher struct{ cl *kgo.Client }

// NewPublisher creates a Publisher with the given franz-go options.
func NewPublisher(opts ...kgo.Opt) (*Publisher, error) {
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Publisher{cl}, nil
}

// Publish returns only after every record is acked, or the first error.
func (p *Publisher) Publish(ctx context.Context, rs ...*kgo.Record) error {
	// ProduceSync returns after every record has a result; FirstErr is that
	// result, so a nil return means the broker acked all of them.
	return p.cl.ProduceSync(ctx, rs...).FirstErr()
}

// Close releases the Publisher's resources.
func (p *Publisher) Close() { p.cl.Close() }
