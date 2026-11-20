// Package harness holds the black-box test helpers shared by
// offsetkeeper/contract_test.go and test/edge. Import it only from external test packages: it imports
// offsetkeeper, so an in-package test importing it would be a cycle.
package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
	"github.com/AynSiddiqui/offsetkeeper/offsetkeeper"
)

// Every test runs under testTimeout; waits inside use shorter limits.
const (
	testTimeout = 45 * time.Second
	WaitFor     = 30 * time.Second
)

// ErrBoom is the generic handler failure used across tests.
var ErrBoom = errors.New("boom")

// Ctx returns a context bounded by the per-test timeout.
func Ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// Env is a test topic, group and brokers.
type Env struct {
	Brokers      []string
	Topic, Group string
	Partitions   int32
}

// NewEnv creates a topic with the given partitions and a fresh group.
func NewEnv(t *testing.T, partitions int32) Env {
	t.Helper()
	b := testenv.Brokers(t)
	return Env{b, testenv.Topic(t, b, partitions), testenv.Group(t), partitions}
}

// Cfg returns a Config for the env with fast retries and a discarded logger.
func (e Env) Cfg(h offsetkeeper.Handler) offsetkeeper.Config {
	return offsetkeeper.Config{
		Brokers: e.Brokers, Group: e.Group, Topics: []string{e.Topic}, Handler: h,
		MaxAttempts: 3, Backoff: time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// Produce writes n records to every partition in partitions (all if empty);
// values are "<prefix>-<partition>-<i>", the key is "key-<i>".
func (e Env) Produce(t *testing.T, prefix string, n int, partitions ...int32) {
	t.Helper()
	if len(partitions) == 0 {
		for p := range e.Partitions {
			partitions = append(partitions, p)
		}
	}
	var rs []*kgo.Record
	for _, p := range partitions {
		for i := range n {
			rs = append(rs, &kgo.Record{
				Topic: e.Topic, Partition: p,
				Key: fmt.Appendf(nil, "key-%d", i), Value: fmt.Appendf(nil, "%s-%d-%d", prefix, p, i),
			})
		}
	}
	testenv.Produce(t, e.Brokers, rs...)
}

// Committed returns the group's committed offset per partition.
func (e Env) Committed(t *testing.T) map[int32]int64 {
	t.Helper()
	return testenv.Committed(t, e.Brokers, e.Group, e.Topic)
}

// Drained reports whether every partition's committed offset equals its end.
func (e Env) Drained(t *testing.T) bool {
	t.Helper()
	c := e.Committed(t)
	for p, end := range testenv.End(t, e.Brokers, e.Topic) {
		if c[p] != end {
			return false
		}
	}
	return true
}

// Total is the number of records in topic.
func (e Env) Total(t *testing.T, topic string) (n int) {
	t.Helper()
	for _, end := range testenv.End(t, e.Brokers, topic) {
		n += int(end)
	}
	return n
}

// Seen is one recorded handler invocation.
type Seen struct {
	Partition int32
	Offset    int64
	Value     string
	Who       string
}

// Calls records handler invocations.
type Calls struct {
	mu sync.Mutex
	xs []Seen
}

// Add records one invocation.
func (c *Calls) Add(r *kgo.Record, who string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xs = append(c.xs, Seen{r.Partition, r.Offset, string(r.Value), who})
}

// Count returns how many recorded calls satisfy f.
func (c *Calls) Count(f func(Seen) bool) (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range c.xs {
		if f(x) {
			n++
		}
	}
	return n
}

// Value counts calls with record value v.
func (c *Calls) Value(v string) int { return c.Count(func(s Seen) bool { return s.Value == v }) }

// Offset counts calls at offset o.
func (c *Calls) Offset(o int64) int { return c.Count(func(s Seen) bool { return s.Offset == o }) }

// Past counts calls beyond offset o.
func (c *Calls) Past(o int64) int { return c.Count(func(s Seen) bool { return s.Offset > o }) }

// Total counts all calls.
func (c *Calls) Total() int { return c.Count(func(Seen) bool { return true }) }

// Offsets lists the offsets seen on partition p.
func (c *Calls) Offsets(p int32) (os []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range c.xs {
		if x.Partition == p {
			os = append(os, x.Offset)
		}
	}
	return os
}

// Runner runs offsetkeeper.Run in a goroutine.
type Runner struct {
	Cancel context.CancelFunc
	Exited chan struct{}
	Err    error // valid once Exited is closed
}

// Start runs offsetkeeper.Run with cfg until the test ends.
func Start(t *testing.T, ctx context.Context, cfg offsetkeeper.Config) *Runner {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	r := &Runner{Cancel: cancel, Exited: make(chan struct{})}
	go func() { r.Err = offsetkeeper.Run(ctx, cfg); close(r.Exited) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.Exited:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return within 15s of cancel")
		}
	})
	return r
}

// Wait returns Run's result, failing t if Run is still going after d.
func (r *Runner) Wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-r.Exited:
		return r.Err
	case <-time.After(d):
		t.Fatalf("Run still running after %s", d)
		return nil
	}
}

// Until waits for cond while Run keeps running; an early exit fails t at once.
func (r *Runner) Until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testenv.Eventually(t, WaitFor, func() bool {
		select {
		case <-r.Exited:
			t.Fatalf("waiting for %s: Run exited early: %v", what, r.Err)
		default:
		}
		return cond()
	}, what)
}

// Running reports whether Run has not returned yet.
func (r *Runner) Running() bool {
	select {
	case <-r.Exited:
		return false
	default:
		return true
	}
}

// Header returns the value of the record header key.
func Header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// MustContain fails t unless s contains every sub.
func MustContain(t *testing.T, what, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("%s %q should contain %q", what, s, sub)
		}
	}
}

// GroupState returns the group's state and member count.
func GroupState(t *testing.T, brokers []string, group string) (state string, members int) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gs, err := kadm.NewClient(cl).DescribeGroups(ctx, group)
	if err != nil || gs[group].Err != nil {
		t.Fatalf("describe group: %v / %v", err, gs[group].Err)
	}
	return gs[group].State, len(gs[group].Members)
}
