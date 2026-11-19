package offsetkeeper_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/AynSiddiqui/offsetkeeper"
	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

// Every test runs under testTimeout; waits inside use shorter limits.
const (
	testTimeout = 45 * time.Second
	waitFor     = 30 * time.Second
)

var errBoom = errors.New("boom")

// fastGroup lets a SIGKILLed member expire quickly on real brokers.
var fastGroup = []kgo.Opt{kgo.SessionTimeout(6 * time.Second)}

// ---- helpers ----------------------------------------------------------------

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

type env struct {
	brokers      []string
	topic, group string
	partitions   int32
}

func newEnv(t *testing.T, partitions int32) env {
	t.Helper()
	b := testenv.Brokers(t)
	return env{b, testenv.Topic(t, b, partitions), testenv.Group(t), partitions}
}

func (e env) cfg(h offsetkeeper.Handler) offsetkeeper.Config {
	return offsetkeeper.Config{
		Brokers: e.brokers, Group: e.group, Topics: []string{e.topic}, Handler: h,
		MaxAttempts: 3, Backoff: time.Millisecond,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

// produce writes n records to every partition in partitions (all if empty);
// values are "<prefix>-<partition>-<i>", the key is "key-<i>".
func (e env) produce(t *testing.T, prefix string, n int, partitions ...int32) {
	t.Helper()
	if len(partitions) == 0 {
		for p := range e.partitions {
			partitions = append(partitions, p)
		}
	}
	var rs []*kgo.Record
	for _, p := range partitions {
		for i := range n {
			rs = append(rs, &kgo.Record{
				Topic: e.topic, Partition: p,
				Key: fmt.Appendf(nil, "key-%d", i), Value: fmt.Appendf(nil, "%s-%d-%d", prefix, p, i),
			})
		}
	}
	testenv.Produce(t, e.brokers, rs...)
}

func (e env) committed(t *testing.T) map[int32]int64 {
	t.Helper()
	return testenv.Committed(t, e.brokers, e.group, e.topic)
}

// drained reports whether every partition's committed offset equals its end.
func (e env) drained(t *testing.T) bool {
	t.Helper()
	c := e.committed(t)
	for p, end := range testenv.End(t, e.brokers, e.topic) {
		if c[p] != end {
			return false
		}
	}
	return true
}

func (e env) total(t *testing.T, topic string) (n int) {
	t.Helper()
	for _, end := range testenv.End(t, e.brokers, topic) {
		n += int(end)
	}
	return n
}

type seen struct {
	partition int32
	offset    int64
	value     string
	who       string
}

// calls records handler invocations.
type calls struct {
	mu sync.Mutex
	xs []seen
}

func (c *calls) add(r *kgo.Record, who string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xs = append(c.xs, seen{r.Partition, r.Offset, string(r.Value), who})
}

func (c *calls) count(f func(seen) bool) (n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range c.xs {
		if f(x) {
			n++
		}
	}
	return n
}

func (c *calls) value(v string) int { return c.count(func(s seen) bool { return s.value == v }) }
func (c *calls) offset(o int64) int { return c.count(func(s seen) bool { return s.offset == o }) }
func (c *calls) past(o int64) int   { return c.count(func(s seen) bool { return s.offset > o }) }
func (c *calls) total() int         { return c.count(func(seen) bool { return true }) }
func (c *calls) offsets(p int32) (os []int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range c.xs {
		if x.partition == p {
			os = append(os, x.offset)
		}
	}
	return os
}

// runner runs offsetkeeper.Run in a goroutine.
type runner struct {
	cancel context.CancelFunc
	exited chan struct{}
	err    error // valid once exited is closed
}

func start(t *testing.T, ctx context.Context, cfg offsetkeeper.Config) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	r := &runner{cancel: cancel, exited: make(chan struct{})}
	go func() { r.err = offsetkeeper.Run(ctx, cfg); close(r.exited) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-r.exited:
		case <-time.After(15 * time.Second):
			t.Error("Run did not return within 15s of cancel")
		}
	})
	return r
}

// wait returns Run's result, failing t if Run is still going after d.
func (r *runner) wait(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case <-r.exited:
		return r.err
	case <-time.After(d):
		t.Fatalf("Run still running after %s", d)
		return nil
	}
}

// until waits for cond while Run keeps running; an early exit fails t at once.
func (r *runner) until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testenv.Eventually(t, waitFor, func() bool {
		select {
		case <-r.exited:
			t.Fatalf("waiting for %s: Run exited early: %v", what, r.err)
		default:
		}
		return cond()
	}, what)
}

func (r *runner) running() bool {
	select {
	case <-r.exited:
		return false
	default:
		return true
	}
}

func header(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

func mustContain(t *testing.T, what, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("%s %q should contain %q", what, s, sub)
		}
	}
}

// ---- tests ------------------------------------------------------------------

// A record that fails after MaxAttempts without a DLQ stops Run; nothing at or
// past it is committed or handled.
func TestNoCommitPastFailedRecord(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 5)
	var c calls
	r := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 2 {
			return errBoom
		}
		return nil
	}))
	err := r.wait(t, waitFor)
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run returned %v, want an error wrapping the handler error", err)
	}
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 2", e.topic)) // names topic, partition and offset; "2" alone matches the topic name digits
	if n := c.offset(2); n != 3 {
		t.Errorf("failed record handled %d times, want MaxAttempts=3", n)
	}
	if n := c.past(2); n != 0 {
		t.Errorf("%d records past the failed one were handled", n)
	}
	// Offsets 0 and 1 settled; Run commits that prefix on its error exit.
	if got := e.committed(t)[0]; got != 2 {
		t.Errorf("committed %d, want exactly 2 (offsets 0-1 settled, 2 failed)", got)
	}
}

const (
	crashEnv     = "OFFSETKEEPER_CRASH_CHILD" // brokers|topic|group; set only in the child
	crashBlockAt = 5
	crashRecords = 10
)

// TestHelperCrashChild is the child of TestCrashBeforeCommitRedelivers: it runs
// Run, prints each handled offset and blocks at crashBlockAt until killed.
// Without crashEnv it does nothing.
func TestHelperCrashChild(t *testing.T) {
	v := os.Getenv(crashEnv)
	if v == "" {
		return
	}
	f := strings.Split(v, "|")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	cfg := offsetkeeper.Config{
		Brokers: strings.Split(f[0], ","), Group: f[2], Topics: []string{f[1]},
		MaxAttempts: 3, Backoff: time.Millisecond, ClientOpts: fastGroup,
		Handler: func(ctx context.Context, r *kgo.Record) error {
			if r.Offset >= crashBlockAt {
				<-ctx.Done()
				return ctx.Err()
			}
			fmt.Printf("handled %d\n", r.Offset)
			return nil
		},
	}
	t.Fatalf("Run returned before being killed: %v", offsetkeeper.Run(ctx, cfg))
}

// A real SIGKILL mid-batch leaves the batch uncommitted; a restart re-handles it.
func TestCrashBeforeCommitRedelivers(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", crashRecords)

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperCrashChild$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), crashEnv+"="+strings.Join([]string{strings.Join(e.brokers, ","), e.topic, e.group}, "|"))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })

	handled := make(chan int64)
	var other []string // child output that is not a "handled" line; read after handled closes
	go func() {
		defer close(handled)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if s, ok := strings.CutPrefix(sc.Text(), "handled "); ok {
				if o, err := strconv.ParseInt(s, 10, 64); err == nil {
					handled <- o
					continue
				}
			}
			other = append(other, sc.Text())
		}
	}()

	got := map[int64]bool{}
	deadline := time.After(waitFor)
	for len(got) < crashBlockAt {
		select {
		case o, ok := <-handled:
			if !ok {
				_ = cmd.Wait()
				t.Fatalf("child exited after handling %d of %d records: %v\nstdout: %q\nstderr: %s",
					len(got), crashBlockAt, cmd.ProcessState, other, stderr.String())
			}
			got[o] = true
		case <-deadline:
			t.Fatalf("child handled only %d of %d records in %s", len(got), crashBlockAt, waitFor)
		}
	}
	if err := cmd.Process.Kill(); err != nil { // SIGKILL: no deferred cleanup, no final commit
		t.Fatal(err)
	}
	for range handled {
	}
	_ = cmd.Wait()

	from := e.committed(t)[0]
	if from > crashBlockAt {
		t.Fatalf("committed %d, past the unsettled record at offset %d", from, crashBlockAt)
	}

	var c calls
	r := start(t, ctx, func() offsetkeeper.Config {
		cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error { c.add(rec, ""); return nil })
		cfg.ClientOpts = fastGroup
		return cfg
	}())
	r.until(t, "restarted Run to drain the topic", func() bool { return e.drained(t) })
	for o := from; o < crashRecords; o++ {
		if c.offset(o) == 0 {
			t.Errorf("offset %d was not redelivered after the crash (committed before restart: %d)", o, from)
		}
	}
}

func groupState(t *testing.T, brokers []string, group string) (state string, members int) {
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

// A member joining while a handler blocks must not get records the first member
// is still handling: the rebalance waits for the batch to be settled and committed.
func TestRebalanceDuringHandler(t *testing.T) {
	const per = 8
	e, ctx := newEnv(t, 4), testCtx(t)
	e.produce(t, "w1", per)

	var c calls
	var blocking atomic.Bool
	blocked, release := make(chan struct{}), make(chan struct{})
	handler := func(who string) offsetkeeper.Handler {
		return func(ctx context.Context, r *kgo.Record) error {
			c.add(r, who)
			if who == "A" && blocking.CompareAndSwap(false, true) {
				close(blocked)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
	}
	a := start(t, ctx, e.cfg(handler("A")))
	a.until(t, "member A to block in a handler", func() bool { return blocking.Load() })
	<-blocked
	b := start(t, ctx, e.cfg(handler("B")))
	a.until(t, "B's join to start a rebalance", func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "PreparingRebalance" || members == 2
	})
	// Hold A's batch open while B gets a chance to (wrongly) be handed A's partitions.
	window, tick := time.NewTimer(time.Second), time.NewTicker(20*time.Millisecond)
	defer tick.Stop()
	for held := true; held; {
		select {
		case <-window.C:
			held = false
		case <-tick.C:
			if n := c.count(func(s seen) bool { return s.who == "B" }); n != 0 {
				t.Fatalf("B handled %d records while A's batch was still open: the rebalance did not wait", n)
			}
		}
	}
	close(release)

	a.until(t, "wave 1 settled and the group stable with 2 members", func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "Stable" && members == 2 && e.drained(t)
	})
	if n := c.count(func(s seen) bool { return s.who == "B" }); n != 0 {
		t.Errorf("B handled %d records of wave 1: the rebalance did not wait for A's batch", n)
	}

	e.produce(t, "w2", 4)
	a.until(t, "wave 2 drained", func() bool { return e.drained(t) })
	b.until(t, "wave 2 drained", func() bool { return e.drained(t) })
	for p := range e.partitions {
		for i := range per {
			if v := fmt.Sprintf("w1-%d-%d", p, i); c.value(v) == 0 {
				t.Errorf("%s never handled", v)
			}
		}
		for i := range 4 {
			if v := fmt.Sprintf("w2-%d-%d", p, i); c.value(v) == 0 {
				t.Errorf("%s never handled", v)
			}
		}
	}
	for _, who := range []string{"A", "B"} {
		if n := c.count(func(s seen) bool { return s.who == who && strings.HasPrefix(s.value, "w2") }); n == 0 {
			t.Errorf("member %s handled no wave-2 records", who)
		}
	}
	for p, end := range testenv.End(t, e.brokers, e.topic) {
		if got := e.committed(t)[p]; got != end {
			t.Errorf("partition %d committed %d, end %d", p, got, end)
		}
	}
}

// Publish returns only after acks, so every record is readable; a missing topic is an error.
func TestProduceIsAcknowledged(t *testing.T) {
	e, ctx := newEnv(t, 2), testCtx(t)
	p, err := offsetkeeper.NewPublisher(kgo.SeedBrokers(e.brokers...), kgo.UnknownTopicRetries(1))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(p.Close)

	var rs []*kgo.Record
	for i := range 5 {
		rs = append(rs, &kgo.Record{Topic: e.topic, Key: fmt.Appendf(nil, "k%d", i), Value: fmt.Appendf(nil, "v%d", i)})
	}
	if err := p.Publish(ctx, rs...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := map[string]bool{}
	for _, r := range testenv.Records(t, e.brokers, e.topic) {
		got[string(r.Key)+"="+string(r.Value)] = true
	}
	for i := range 5 {
		if k := fmt.Sprintf("k%d=v%d", i, i); !got[k] {
			t.Errorf("acked record %s is not readable; have %v", k, got)
		}
	}

	mctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err = p.Publish(mctx, &kgo.Record{Topic: e.topic + ".missing", Value: []byte("x")})
	if err == nil {
		t.Error("Publish to a missing topic returned nil")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Publish to a missing topic hung until the deadline: %v", err)
	}
}

// If the DLQ write fails, the source record stays unsettled and Run returns an error.
func TestFailedDLQPublishDoesNotSettleSource(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 3)
	var c calls
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 1 {
			return errBoom
		}
		return nil
	})
	cfg.DLQTopic = e.topic + ".dlq-missing" // never created
	cfg.ClientOpts = []kgo.Opt{kgo.UnknownTopicRetries(1)}
	err := start(t, ctx, cfg).wait(t, waitFor)
	if err == nil {
		t.Fatal("Run returned nil though the DLQ write failed")
	}
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 1", e.topic))
	if n := c.offset(1); n != 3 {
		t.Errorf("failed record handled %d times, want 3", n)
	}
	if n := c.past(1); n != 0 {
		t.Errorf("%d records past the unsettled one were handled", n)
	}
	// Offset 0 settled; offset 1's DLQ write failed, so it is not.
	if got := e.committed(t)[0]; got != 1 {
		t.Errorf("committed %d, want exactly 1 (offset 0 settled, 1's DLQ write failed)", got)
	}
}

// Cancel mid-batch: settled records are committed, the blocked one is not,
// nothing is retried or started, and Run returns nil.
func TestShutdownDrains(t *testing.T) { shutdownDrains(t, false) }

// Same, with DLQTopic set: a handler that fails only because of the cancel must
// not be dead-lettered, so the DLQ stays empty and the commit is unchanged.
func TestShutdownDrainsWithDLQ(t *testing.T) { shutdownDrains(t, true) }

func shutdownDrains(t *testing.T, withDLQ bool) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := ""
	if withDLQ {
		dlqTopic = testenv.Topic(t, e.brokers, 1)
	}
	e.produce(t, "r", 6)
	var c calls
	var reached atomic.Bool
	cfg := e.cfg(func(ctx context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset < 3 {
			return nil
		}
		reached.Store(true)
		<-ctx.Done() // the handler must receive Run's ctx
		return ctx.Err()
	})
	cfg.DLQTopic = dlqTopic
	r := start(t, ctx, cfg)
	r.until(t, "handler to block at offset 3", reached.Load)
	r.cancel()
	if err := r.wait(t, 15*time.Second); err != nil {
		t.Fatalf("Run returned %v after cancel, want nil", err)
	}
	if got := e.committed(t)[0]; got != 3 {
		t.Errorf("committed %d, want 3 (offsets 0-2 settled)", got)
	}
	if n := c.offset(3); n != 1 {
		t.Errorf("record 3 handled %d times, want 1 (no retry after cancel)", n)
	}
	if n := c.past(3); n != 0 {
		t.Errorf("%d records after the blocked one were handled after cancel", n)
	}
	if withDLQ {
		if n := e.total(t, dlqTopic); n != 0 {
			t.Errorf("DLQ has %d records after shutdown, want 0 (a cancel is not a bad record)", n)
		}
	}
}

// eventLog records produce acks and OffsetCommit writes in the order they happen.
type eventLog struct {
	mu sync.Mutex
	xs []string
}

func (l *eventLog) add(s string) { l.mu.Lock(); l.xs = append(l.xs, s); l.mu.Unlock() }
func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.xs...)
}
func (l *eventLog) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if err == nil {
		l.add("ack:" + r.Topic)
	}
}
func (l *eventLog) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if err == nil && key == (&kmsg.OffsetCommitRequest{}).Key() {
		l.add("commit")
	}
}

// The DLQ ack precedes any offset commit, and the DLQ record carries the source
// key, value, headers and the offsetkeeper.* headers.
func TestDLQWriteConfirmedBeforeSourceCommit(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	hdrs := []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}, {Key: "h2", Value: []byte("v2")}}
	testenv.Produce(t, e.brokers,
		&kgo.Record{Topic: e.topic, Value: []byte("ok-0")},
		&kgo.Record{Topic: e.topic, Key: []byte("k1"), Value: []byte("poison"), Headers: hdrs},
		&kgo.Record{Topic: e.topic, Value: []byte("ok-2")})

	log := &eventLog{}
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if string(rec.Value) == "poison" {
			return errors.New("kaboom")
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	cfg.ClientOpts = []kgo.Opt{kgo.WithHooks(log)}
	r := start(t, ctx, cfg)
	r.until(t, "source drained", func() bool { return e.drained(t) })

	ack, commit := -1, -1
	for i, ev := range log.snapshot() {
		switch {
		case ev == "ack:"+dlqTopic && ack < 0:
			ack = i
		case ev == "commit" && commit < 0:
			commit = i
		}
	}
	if ack < 0 || commit < 0 || ack > commit {
		t.Errorf("want DLQ ack before first OffsetCommit, got events %v", log.snapshot())
	}

	recs := testenv.Records(t, e.brokers, dlqTopic)
	if len(recs) != 1 {
		t.Fatalf("DLQ has %d records, want 1", len(recs))
	}
	d := recs[0]
	if string(d.Key) != "k1" || string(d.Value) != "poison" {
		t.Errorf("DLQ key/value = %q/%q, want k1/poison", d.Key, d.Value)
	}
	// Literal names on purpose: renaming a constant must fail this test.
	want := map[string]string{
		"h1": "v1", "h2": "v2",
		"offsetkeeper.source.topic": e.topic, "offsetkeeper.source.partition": "0", "offsetkeeper.source.offset": "1",
		"offsetkeeper.attempts": "3",
	}
	for k, v := range want {
		if got, ok := header(d, k); !ok || got != v {
			t.Errorf("DLQ header %s = %q (present %v), want %q", k, got, ok, v)
		}
	}
	if got, _ := header(d, "offsetkeeper.error"); !strings.Contains(got, "kaboom") {
		t.Errorf("DLQ header offsetkeeper.error = %q, want it to contain the handler error", got)
	}
}

// Replaying a dead-lettered record sends it to the handler again; ReplayAll
// remembers what it replayed.
func TestReplayedRecordIsProcessedOnceMore(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	testenv.Produce(t, e.brokers,
		&kgo.Record{Topic: e.topic, Key: []byte("a"), Value: []byte("poison-1"), Headers: []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}}},
		&kgo.Record{Topic: e.topic, Key: []byte("b"), Value: []byte("poison-2"), Headers: []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}}})

	var fixed, badHeaders atomic.Bool
	var ok calls
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if !fixed.Load() {
			return errBoom
		}
		ok.add(rec, "")
		if _, has := header(rec, dlq.HeaderAttempts); has {
			badHeaders.Store(true) // offsetkeeper.* must be stripped on replay
		}
		if v, _ := header(rec, "h1"); v != "v1" {
			badHeaders.Store(true)
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	r := start(t, ctx, cfg)
	r.until(t, "both records dead-lettered", func() bool { return e.total(t, dlqTopic) == 2 && e.drained(t) })
	fixed.Store(true)

	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	if err := dlq.ReplayOne(ctx, cl, dlqTopic, 0, 0); err != nil {
		t.Fatalf("ReplayOne: %v", err)
	}
	r.until(t, "poison-1 handled after ReplayOne", func() bool { return ok.value("poison-1") >= 1 })

	// ReplayOne is independent of ReplayAll's tracking (internal/dlq, CONTRACT.md
	// guarantee 7): poison-1 was replayed by ReplayOne and is replayed again here.
	n, err := dlq.ReplayAll(ctx, cl, dlqTopic)
	if err != nil || n != 2 {
		t.Fatalf("first ReplayAll = %d, %v; want exactly 2 (both records, ReplayOne is not tracked)", n, err)
	}
	r.until(t, "poison-2 handled after ReplayAll", func() bool { return ok.value("poison-2") >= 1 && e.drained(t) })
	if badHeaders.Load() {
		t.Error("a replayed record kept offsetkeeper.* headers or lost its original headers")
	}

	end := testenv.End(t, e.brokers, e.topic)
	n, err = dlq.ReplayAll(ctx, cl, dlqTopic)
	if err != nil || n != 0 {
		t.Fatalf("second ReplayAll = %d, %v; want 0", n, err)
	}
	if after := testenv.End(t, e.brokers, e.topic); after[0] != end[0] {
		t.Errorf("second ReplayAll produced records: end %v -> %v", end, after)
	}
}

// Per partition: offsets strictly increase and handlers never overlap.
func TestPartitionOrder(t *testing.T) {
	const parts, per = 4, 20
	e, ctx := newEnv(t, parts), testCtx(t)
	e.produce(t, "r", per)
	var c calls
	var inflight [parts]atomic.Int32
	var overlap atomic.Bool
	r := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if inflight[rec.Partition].Add(1) > 1 {
			overlap.Store(true)
		}
		c.add(rec, "")
		for range 20 {
			runtime.Gosched() // widen the window in which an overlap would show
		}
		inflight[rec.Partition].Add(-1)
		return nil
	}))
	r.until(t, "topic drained", func() bool { return e.drained(t) })
	if overlap.Load() {
		t.Error("two handlers ran at once in one partition")
	}
	for p := range int32(parts) {
		os := c.offsets(p)
		if len(os) != per {
			t.Errorf("partition %d: %d calls, want %d", p, len(os), per)
		}
		for i := 1; i < len(os); i++ {
			if os[i] <= os[i-1] {
				t.Errorf("partition %d: offset %d handled after %d", p, os[i], os[i-1])
			}
		}
	}
}

// Permanent skips the retries: one attempt, then the DLQ with attempts=1.
func TestPermanentSkipsRetries(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	e.produce(t, "r", 3)
	var c calls
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 1 {
			return offsetkeeper.Permanent(errBoom)
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	r := start(t, ctx, cfg)
	r.until(t, "record dead-lettered and source drained", func() bool { return e.total(t, dlqTopic) == 1 && e.drained(t) })
	if n := c.offset(1); n != 1 {
		t.Errorf("permanent failure handled %d times, want 1", n)
	}
	recs := testenv.Records(t, e.brokers, dlqTopic)
	if got, _ := header(recs[0], dlq.HeaderAttempts); got != "1" {
		t.Errorf("DLQ attempts = %q, want 1", got)
	}
	if !r.running() {
		t.Errorf("Run exited: %v", r.err)
	}
}

// A handler panic is retried like an error, dead-lettered with its text, and
// does not take the process or the partition down.
func TestHandlerPanicIsAnError(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	e.produce(t, "r", 3)
	var c calls
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 1 {
			panic("kaboom")
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	r := start(t, ctx, cfg)
	r.until(t, "panicking record dead-lettered and source drained", func() bool { return e.total(t, dlqTopic) == 1 && e.drained(t) })
	if n := c.offset(1); n != 3 {
		t.Errorf("panicking record handled %d times, want 3", n)
	}
	if c.offset(2) == 0 {
		t.Error("record after the panic was never handled")
	}
	if got, _ := header(testenv.Records(t, e.brokers, dlqTopic)[0], dlq.HeaderError); !strings.Contains(got, "kaboom") {
		t.Errorf("DLQ error header = %q, want the panic value", got)
	}
	if !r.running() {
		t.Errorf("Run exited: %v", r.err)
	}
}

// Side effects do not settle a record: only the return value does.
func TestHandlerReturnOwnsSettlement(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 3)
	var c calls
	var sideEffects atomic.Int32
	err := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		sideEffects.Add(1) // work done before failing
		return errBoom
	})).wait(t, waitFor)
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run returned %v, want an error wrapping the handler error", err)
	}
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 0", e.topic))
	if n := sideEffects.Load(); n != 3 {
		t.Errorf("side effect ran %d times, want 3", n)
	}
	if n := c.past(0); n != 0 {
		t.Errorf("%d records after the failed first record were handled", n)
	}
	if got := e.committed(t)[0]; got != 0 {
		t.Errorf("committed %d though the handler returned an error", got)
	}
}

// The Kafka key is routing data, not an identity: same-key records are all handled.
func TestKafkaKeyIsNotMessageID(t *testing.T) {
	const n = 10
	e, ctx := newEnv(t, 1), testCtx(t)
	var rs []*kgo.Record
	for i := range n {
		v := fmt.Sprintf("v%d", i)
		if i >= n-2 {
			v = "same" // two records identical in key and value
		}
		rs = append(rs, &kgo.Record{Topic: e.topic, Key: []byte("dup"), Value: []byte(v)})
	}
	testenv.Produce(t, e.brokers, rs...)
	var c calls
	r := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error { c.add(rec, ""); return nil }))
	r.until(t, "topic drained", func() bool { return e.drained(t) })
	for o := range int64(n) {
		if c.offset(o) == 0 {
			t.Errorf("offset %d was never handled", o)
		}
	}
}

// Cancel during a long backoff: no further attempt, and Run returns promptly with nil.
func TestRetryBackoffStopsOnShutdown(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 1)
	var c calls
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error { c.add(rec, ""); return errBoom })
	cfg.MaxAttempts, cfg.Backoff = 5, 10*time.Second
	r := start(t, ctx, cfg)
	r.until(t, "first attempt", func() bool { return c.total() >= 1 })
	r.cancel()
	if err := r.wait(t, 5*time.Second); err != nil { // < Backoff: Run must not sit out the delay
		t.Fatalf("Run returned %v after cancel, want nil", err)
	}
	if n := c.total(); n != 1 {
		t.Errorf("handler ran %d times, want 1 (no retry after cancel)", n)
	}
	if got := e.committed(t)[0]; got != 0 {
		t.Errorf("committed %d for an unsettled record", got)
	}
}

// A failed OffsetCommit makes Run return that error. Needs fault injection, so kfake only.
func TestCommitErrorStopsRun(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects an OffsetCommit failure with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 3)

	// Non-retriable, so kgo hands it to Run instead of retrying it away.
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
		for _, rt := range req.(*kmsg.OffsetCommitRequest).Topics {
			st := kmsg.NewOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.OffsetMetadataTooLarge.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	var c calls
	err := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error { c.add(rec, ""); return nil })).wait(t, waitFor)
	if !errors.Is(err, kerr.OffsetMetadataTooLarge) {
		t.Fatalf("Run returned %v, want the commit error", err)
	}
	if got := e.committed(t); len(got) != 0 {
		t.Errorf("committed %v though every commit failed", got)
	}
}

// A handler that ignores ctx and returns nil after the cancel: that record is
// settled and committed, but no later record is handled.
func TestNoRecordHandledAfterCancel(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 5)
	var c calls
	var reached atomic.Bool
	r := start(t, ctx, e.cfg(func(ctx context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 1 {
			reached.Store(true)
			<-ctx.Done()
		}
		return nil // nil even after the cancel: the loop itself must stop
	}))
	r.until(t, "handler to block at offset 1", reached.Load)
	r.cancel()
	if err := r.wait(t, 15*time.Second); err != nil {
		t.Fatalf("Run returned %v after cancel, want nil", err)
	}
	if n := c.past(1); n != 0 {
		t.Errorf("%d records after the cancelled one were handled", n)
	}
	if got := e.committed(t)[0]; got != 2 {
		t.Errorf("committed %d, want 2 (offsets 0-1 returned nil)", got)
	}
}

// runtime.Goexit (t.FailNow, a library) is not a panic and recover() cannot stop
// it; the partition goroutine must count as failed, not as "no error".
func TestHandlerGoexitStopsRun(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 4)
	var c calls
	err := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		if rec.Offset == 1 {
			runtime.Goexit()
		}
		return nil
	})).wait(t, waitFor)
	if err == nil {
		t.Fatal("Run returned nil though a handler never returned")
	}
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 1", e.topic), "Goexit")
	if n := c.past(1); n != 0 {
		t.Errorf("%d records past the dead handler were handled", n)
	}
	// Offset 0 settled before the Goexit and is kept; offset 1 never returned.
	if got := e.committed(t)[0]; got != 1 {
		t.Errorf("committed %d, want exactly 1", got)
	}
}

// One partition fails while the other is healthy: Run's error exit still commits
// the healthy partition's settled prefix.
func TestErrorExitCommitsOtherPartitions(t *testing.T) {
	e, ctx := newEnv(t, 2), testCtx(t)
	e.produce(t, "r", 5)
	var c calls
	var p1Done atomic.Int64 // records partition 1 returned nil for
	var lastOK atomic.Int64 // highest such offset
	lastOK.Store(-1)
	r := start(t, ctx, e.cfg(func(ctx context.Context, rec *kgo.Record) error {
		c.add(rec, "")
		switch {
		case rec.Partition == 1 && rec.Offset < 2:
			lastOK.Store(rec.Offset)
			p1Done.Add(1)
			return nil
		case rec.Partition == 1:
			<-ctx.Done() // stays unsettled until the failure cancels the batch
			return ctx.Err()
		case rec.Offset == 2:
			// Fail only after partition 1 has settled two records, so the prefix
			// to commit exists; bounded so a split poll fails loudly, not by hanging.
			testenv.Eventually(t, 10*time.Second, func() bool { return p1Done.Load() >= 2 }, "partition 1 to settle 2 records (are both partitions in one poll?)")
			return errBoom
		}
		return nil
	}))
	err := r.wait(t, waitFor)
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run returned %v, want the handler error", err)
	}
	got := e.committed(t)
	if got[0] != 2 {
		t.Errorf("failing partition committed %d, want 2", got[0])
	}
	if ok := lastOK.Load(); got[1] < ok+1 || got[1] < 2 {
		t.Errorf("healthy partition committed %d, want >= %d (its settled prefix)", got[1], ok+1)
	}
}

// A retriable DLQ produce error (here NOT_ENOUGH_REPLICAS) is retried by kgo
// forever; the DLQ timeout turns it into an error so Run does not hang holding
// the poll gate. kfake only: a real broker cannot be told to fail.
func TestDLQProduceTimeoutStopsRun(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects NOT_ENOUGH_REPLICAS with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)
	e.produce(t, "r", 3)
	// Registered before start, so it runs after start's cleanup has seen Run exit.
	t.Cleanup(offsetkeeper.SetDLQTimeout(500 * time.Millisecond))

	var attempts atomic.Int32
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		attempts.Add(1)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range req.(*kmsg.ProduceRequest).Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotEnoughReplicas.Code // retriable
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 1 {
			return errBoom
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	err := start(t, ctx, cfg).wait(t, 15*time.Second)
	if err == nil {
		t.Fatal("Run returned nil though the DLQ produce never succeeded")
	}
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 1", e.topic), dlqTopic)
	if attempts.Load() == 0 {
		t.Error("the injected Produce error was never hit: the test proves nothing")
	}
	if got := e.committed(t)[0]; got != 1 {
		t.Errorf("committed %d, want exactly 1 (offset 0 settled, 1's DLQ write failed)", got)
	}
}

// A handler that re-publishes the record it was given (a "retry later" pattern)
// makes kgo rewrite that record's Offset; the commit must not follow it.
func TestHandlerCannotMoveCommit(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "r", 3)
	p, err := offsetkeeper.NewPublisher(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(p.Close)
	cfg := e.cfg(func(ctx context.Context, rec *kgo.Record) error {
		if rec.Offset == 1 {
			return errBoom
		}
		return p.Publish(ctx, rec) // moves rec.Offset past the end of the topic
	})
	if err := start(t, ctx, cfg).wait(t, waitFor); err == nil {
		t.Fatal("Run returned nil though offset 1 failed without a DLQ")
	}
	if got := e.committed(t)[0]; got != 1 {
		t.Errorf("committed %d, want exactly 1 (only offset 0 settled)", got)
	}
}

// A handler that rewrites the record's routing fields before failing must not
// change what the DLQ says about the source.
func TestDLQNamesOriginalSource(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	e.produce(t, "r", 3)
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 1 {
			rec.Topic, rec.Partition, rec.Offset = "elsewhere", 7, 99
			return errBoom
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	start(t, ctx, cfg).until(t, "source drained", func() bool { return e.drained(t) })

	recs := testenv.Records(t, e.brokers, dlqTopic)
	if len(recs) != 1 {
		t.Fatalf("DLQ has %d records, want 1", len(recs))
	}
	want := map[string]string{
		dlq.HeaderSourceTopic: e.topic, dlq.HeaderSourcePartition: "0", dlq.HeaderSourceOffset: "1",
	}
	for k, v := range want {
		if got, _ := header(recs[0], k); got != v {
			t.Errorf("DLQ header %s = %q, want %q", k, got, v)
		}
	}
}

// produceSeen is what one produce request told the broker.
type produceSeen struct {
	acks       int16
	producerID int64
}

// observeProduce records every Produce request the cluster gets, passing it on
// to the normal handler. The returned func returns and clears what was seen
// since the last call. Requests carry a topic ID, not a name, on current Kafka
// versions, so callers separate producers by phase instead of by topic.
func observeProduce(t *testing.T, cluster *kfake.Cluster) func() []produceSeen {
	var (
		mu   sync.Mutex
		seen []produceSeen
	)
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		pr := req.(*kmsg.ProduceRequest)
		mu.Lock()
		defer mu.Unlock()
		for _, rt := range pr.Topics {
			for _, rp := range rt.Partitions {
				var b kmsg.RecordBatch // the first batch is enough: one client uses one producer ID
				if err := b.ReadFrom(rp.Records); err != nil {
					t.Errorf("decode produce batch for %s: %v", rt.Topic, err)
					continue
				}
				seen = append(seen, produceSeen{pr.Acks, b.ProducerID})
			}
		}
		return nil, nil, false
	})
	return func() []produceSeen {
		mu.Lock()
		defer mu.Unlock()
		out := seen
		seen = nil
		return out
	}
}

func checkAcksAll(t *testing.T, what string, got []produceSeen) {
	t.Helper()
	if len(got) == 0 {
		t.Fatalf("%s: broker saw no produce request", what)
	}
	for _, s := range got {
		if s.acks != -1 || s.producerID < 0 {
			t.Errorf("%s: produce acks=%d producerID=%d, want acks=-1 and producerID>=0 (idempotent)", what, s.acks, s.producerID)
		}
	}
}

// Publisher and the DLQ produce use acks=all with an idempotent producer on
// default options; the broker is the witness. kfake only: it reads the produce
// requests through ControlKey, which a real broker has no equivalent for.
func TestPublishUsesAcksAllAndIdempotence(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("reads produce requests with kfake's ControlKey; a real broker cannot be observed")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)
	seen := observeProduce(t, cluster)

	p, err := offsetkeeper.NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(p.Close)
	if err := p.Publish(ctx, &kgo.Record{Topic: e.topic, Value: []byte("x")}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	checkAcksAll(t, "Publisher.Publish", seen())

	e.produce(t, "r", 3)
	seen() // the test's own seeding produce is not under test
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 1 {
			return errBoom
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	start(t, ctx, cfg).until(t, "dead-lettered and drained", func() bool { return e.drained(t) })
	checkAcksAll(t, "DLQ produce", seen()) // Run produces only the dead letter
}

// A handler error far larger than the broker accepts must not make the DLQ
// record unwritable: the header is capped, so the record is dead-lettered and
// the source commit moves on.
func TestDLQErrorHeaderCapped(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)
	e.produce(t, "r", 2)
	huge := errors.New(strings.Repeat("x", 2<<20))
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 1 {
			return huge
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	start(t, ctx, cfg).until(t, "source drained", func() bool { return e.drained(t) })

	recs := testenv.Records(t, e.brokers, dlqTopic)
	if len(recs) != 1 {
		t.Fatalf("DLQ has %d records, want 1", len(recs))
	}
	got, _ := header(recs[0], "offsetkeeper.error")
	if len(got) > 4096 || !strings.HasSuffix(got, "…(truncated)") {
		t.Errorf("DLQ error header is %d bytes, want <= 4096 ending in the truncation marker", len(got))
	}
	if c := e.committed(t)[0]; c != 2 {
		t.Errorf("committed %d, want 2", c)
	}
}
