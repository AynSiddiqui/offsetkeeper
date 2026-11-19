package offsetkeeper

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

func validConfig() Config {
	return Config{
		Brokers: []string{"b:9092"},
		Group:   "g",
		Topics:  []string{"t"},
		Handler: func(context.Context, *kgo.Record) error { return nil },
	}
}

func TestConfigDefaults(t *testing.T) {
	c, err := validConfig().withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxAttempts != 3 || c.Backoff != 100*time.Millisecond || c.Logger != slog.Default() {
		t.Fatalf("bad defaults: %+v", c)
	}
	in := validConfig()
	in.MaxAttempts, in.Backoff = 1, time.Second
	c, _ = in.withDefaults()
	if c.MaxAttempts != 1 || c.Backoff != time.Second {
		t.Fatalf("explicit values overwritten: %+v", c)
	}
}

func TestConfigValidation(t *testing.T) {
	for name, mut := range map[string]func(*Config){
		"brokers":       func(c *Config) { c.Brokers = nil },
		"group":         func(c *Config) { c.Group = "" },
		"topics":        func(c *Config) { c.Topics = nil },
		"handler":       func(c *Config) { c.Handler = nil },
		"maxattempts":   func(c *Config) { c.MaxAttempts = -1 },
		"backoff":       func(c *Config) { c.Backoff = -1 },
		"dlq in topics": func(c *Config) { c.DLQTopic = c.Topics[0] },
	} {
		c := validConfig()
		mut(&c)
		if _, err := c.withDefaults(); err == nil {
			t.Errorf("%s: want error", name)
		} else if name == "dlq in topics" && !strings.Contains(err.Error(), "Config.DLQTopic") {
			t.Errorf("%s: error %q should name Config.DLQTopic", name, err)
		}
		// A short deadline so a missing check fails here instead of hanging on the fake broker.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := Run(ctx, c); err == nil {
			t.Errorf("%s: Run must reject invalid config, got %v", name, err)
		}
		cancel()
	}
}

type customErr struct{}

func (*customErr) Error() string { return "custom" }

func TestPermanent(t *testing.T) {
	if Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	base := fs.ErrNotExist
	err := Permanent(base)
	var p *permanentError
	if !errors.Is(err, base) || !errors.As(err, &p) || err.Error() != base.Error() {
		t.Fatalf("Is/As/Error broken: %v", err)
	}
	var ce *customErr
	if !errors.As(Permanent(&customErr{}), &ce) {
		t.Fatal("errors.As cannot see the wrapped type")
	}
}

// kgo may hand one partition's records out in several chunks; batch must still
// run one goroutine per partition, or a later chunk would be handled and
// committed past an earlier chunk's failure.
func TestBatchSplitFetchSamePartition(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic, group := testenv.Topic(t, brokers, 1), testenv.Group(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(cl.Close)

	chunk := func(offsets ...int64) kgo.Fetch {
		fp := kgo.FetchPartition{Partition: 0}
		for _, o := range offsets {
			fp.Records = append(fp.Records, &kgo.Record{Topic: topic, Partition: 0, Offset: o})
		}
		return kgo.Fetch{Topics: []kgo.FetchTopic{{Topic: topic, Partitions: []kgo.FetchPartition{fp}}}}
	}
	fetches := kgo.Fetches{chunk(0, 1), chunk(2, 3)}
	errBoom := errors.New("boom")

	var handled sync.Map // offset -> true
	later := make(chan struct{})
	var once sync.Once
	w := &worker{cl: cl, cfg: Config{
		MaxAttempts: 1, Backoff: time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Handler: func(_ context.Context, r *kgo.Record) error {
			handled.Store(r.Offset, true)
			if r.Offset >= 2 {
				once.Do(func() { close(later) })
			}
			if r.Offset == 1 {
				// Give a concurrent goroutine for the second chunk time to run past us.
				select {
				case <-later:
				case <-time.After(300 * time.Millisecond):
				}
				return errBoom
			}
			return nil
		},
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.batch(ctx, fetches); !errors.Is(err, errBoom) {
		t.Fatalf("batch returned %v, want the handler error", err)
	}
	for _, o := range []int64{2, 3} {
		if _, ok := handled.Load(o); ok {
			t.Errorf("offset %d was handled though offset 1 failed", o)
		}
	}
	if got := testenv.Committed(t, brokers, group, topic)[0]; got != 1 {
		t.Errorf("committed %d, want 1 (only offset 0 settled)", got)
	}
}
