package offsetkeeper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

// Case 1: MaxAttempts=1 (no retry, straight to DLQ).
// docs/CONTRACT.md Guarantee 3 & Config.MaxAttempts doc:
// MaxAttempts=1 means no retries; failed record goes straight to DLQ on attempt 1.
func TestEdgeMaxAttemptsOneDLQ(t *testing.T) {
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)
	e.produce(t, "r", 2)

	var calls0, calls1 atomic.Int32
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 0 {
			calls0.Add(1)
			return errBoom
		}
		calls1.Add(1)
		return nil
	})
	cfg.MaxAttempts = 1
	cfg.DLQTopic = dlqTopic

	r := start(t, ctx, cfg)
	r.until(t, "both offsets settled and committed", func() bool {
		return e.committed(t)[0] == 2
	})
	r.cancel()
	if err := r.wait(t, 10*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if n := calls0.Load(); n != 1 {
		t.Errorf("offset 0 handler called %d times, want exactly 1 (no retry)", n)
	}
	if n := calls1.Load(); n != 1 {
		t.Errorf("offset 1 handler called %d times, want exactly 1", n)
	}

	dlqRecs := testenv.Records(t, e.brokers, dlqTopic)
	if len(dlqRecs) != 1 {
		t.Fatalf("DLQ has %d records, want 1", len(dlqRecs))
	}
	if att, ok := header(dlqRecs[0], "offsetkeeper.attempts"); !ok || att != "1" {
		t.Errorf("DLQ attempts header = %q (ok=%v), want %q", att, ok, "1")
	}
}

// Case 2: exact backoff schedule observed through handler call timestamps.
// docs/CONTRACT.md Guarantee 3: doubling backoff from Backoff.
// Review fix: Uses testing/synctest on retry with no broker to assert cumulative virtual times.
func TestEdgeBackoffSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		var callTimes []time.Duration
		startT := time.Now()

		attempts, err := retry(ctx, 4, 20*time.Millisecond, func(ctx context.Context) error {
			callTimes = append(callTimes, time.Since(startT))
			return errors.New("boom")
		})

		if attempts != 4 {
			t.Fatalf("attempts = %d, want 4", attempts)
		}
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if len(callTimes) != 4 {
			t.Fatalf("len(callTimes) = %d, want 4", len(callTimes))
		}

		// Initial attempt at t=0
		if callTimes[0] != 0 {
			t.Errorf("call 0 at %v, want 0", callTimes[0])
		}
		// Attempt 1 after 20ms backoff
		if callTimes[1] != 20*time.Millisecond {
			t.Errorf("call 1 at %v, want 20ms", callTimes[1])
		}
		// Attempt 2 after +40ms backoff (cumulative 60ms)
		if callTimes[2] != 60*time.Millisecond {
			t.Errorf("call 2 at %v, want 60ms", callTimes[2])
		}
		// Attempt 3 after +80ms backoff (cumulative 140ms)
		if callTimes[3] != 140*time.Millisecond {
			t.Errorf("call 3 at %v, want 140ms", callTimes[3])
		}

		if total := time.Since(startT); total != 140*time.Millisecond {
			t.Errorf("total virtual elapsed = %v, want 140ms", total)
		}
	})
}

// Case 3a: permanent error on the last attempt (with DLQ).
// docs/CONTRACT.md Guarantee 3: After the last attempt, with DLQTopic the DLQ record is produced.
func TestEdgePermanentErrorOnLastAttemptDLQ(t *testing.T) {
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)
	e.produce(t, "r", 2)

	var calls0 atomic.Int32
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 0 {
			att := calls0.Add(1)
			if att < 3 {
				return errBoom
			}
			return Permanent(errBoom)
		}
		return nil
	})
	cfg.MaxAttempts = 3
	cfg.DLQTopic = dlqTopic

	r := start(t, ctx, cfg)
	r.until(t, "both offsets settled and committed", func() bool {
		return e.committed(t)[0] == 2
	})
	r.cancel()
	if err := r.wait(t, 10*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if n := calls0.Load(); n != 3 {
		t.Errorf("offset 0 handler called %d times, want 3", n)
	}

	dlqRecs := testenv.Records(t, e.brokers, dlqTopic)
	if len(dlqRecs) != 1 {
		t.Fatalf("DLQ has %d records, want 1", len(dlqRecs))
	}
	if att, ok := header(dlqRecs[0], "offsetkeeper.attempts"); !ok || att != "3" {
		t.Errorf("DLQ attempts header = %q (ok=%v), want %q", att, ok, "3")
	}
}

// Case 3b: permanent error on the last attempt (without DLQ).
// docs/CONTRACT.md Guarantee 3: Without DLQTopic, Run returns an error naming topic, partition, offset.
// Review fix: Asserts formatted source identity, that record 1 was untouched, and checkpoint 0.
func TestEdgePermanentErrorOnLastAttemptNoDLQ(t *testing.T) {
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 2)

	var calls0, calls1 atomic.Int32
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 0 {
			att := calls0.Add(1)
			if att < 3 {
				return errBoom
			}
			return Permanent(errBoom)
		}
		calls1.Add(1)
		return nil
	})
	cfg.MaxAttempts = 3
	cfg.DLQTopic = ""

	r := start(t, ctx, cfg)
	err := r.wait(t, 10*time.Second)
	if err == nil {
		t.Fatal("Run returned nil, want error")
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("Run error %v does not wrap errBoom", err)
	}
	// Assert formatted source identity: topic/partition offset failed after attempts
	mustContain(t, "Run error", err.Error(), fmt.Sprintf("%s/0 offset 0 failed after 3 attempt(s)", e.topic))

	if n := calls0.Load(); n != 3 {
		t.Errorf("offset 0 handler called %d times, want 3", n)
	}
	// Subsequent record in partition must remain completely untouched
	if n := calls1.Load(); n != 0 {
		t.Errorf("offset 1 handler called %d times, want 0 (untouched)", n)
	}
	if got := e.committed(t)[0]; got != 0 {
		t.Errorf("committed %d, want 0", got)
	}
}

// Case 4: handler that fails N-1 times then succeeds (no DLQ, committed).
// docs/CONTRACT.md Guarantee 3: handler returning nil settles the record; no DLQ record produced.
func TestEdgeHandlerFailsNMinusOneThenSucceeds(t *testing.T) {
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)
	e.produce(t, "r", 2)

	var calls0, calls1 atomic.Int32
	cfg := e.cfg(func(_ context.Context, rec *kgo.Record) error {
		if rec.Offset == 0 {
			if calls0.Add(1) < 3 {
				return errBoom
			}
			return nil
		}
		calls1.Add(1)
		return nil
	})
	cfg.MaxAttempts = 3
	cfg.DLQTopic = dlqTopic

	r := start(t, ctx, cfg)
	r.until(t, "both records settled and committed", func() bool {
		return e.committed(t)[0] == 2
	})
	r.cancel()
	if err := r.wait(t, 10*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	if n := calls0.Load(); n != 3 {
		t.Errorf("offset 0 handler called %d times, want 3", n)
	}
	if n := calls1.Load(); n != 1 {
		t.Errorf("offset 1 handler called %d times, want 1", n)
	}

	dlqRecs := testenv.Records(t, e.brokers, dlqTopic)
	if len(dlqRecs) != 0 {
		t.Errorf("DLQ has %d records, want 0 (recovered on 3rd attempt)", len(dlqRecs))
	}
}

// Case 5: DLQ produce fails transiently then succeeds within dlqTimeout.
// docs/CONTRACT.md Guarantee 3: DLQ record produce ack settles the source.
// kfake ControlKey fails first 2 Produce requests to DLQ topic with retriable error;
// 3rd succeeds: record settled once, exactly 1 DLQ record.
func TestEdgeDLQProduceTransientFailure(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects transient Produce error with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	topics, err := kadm.NewClient(cl).ListTopics(ctx, dlqTopic)
	if err != nil {
		t.Fatal(err)
	}
	dlqID := topics[dlqTopic].ID

	e.produce(t, "r", 1)

	var dlqAttempts atomic.Int32
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		pr := req.(*kmsg.ProduceRequest)
		isDLQ := false
		for _, rt := range pr.Topics {
			if rt.Topic == dlqTopic || (dlqID != [16]byte{} && rt.TopicID == dlqID) {
				isDLQ = true
				break
			}
		}
		if !isDLQ {
			return nil, nil, false
		}
		if dlqAttempts.Add(1) <= 2 {
			resp := req.ResponseKind().(*kmsg.ProduceResponse)
			for _, rt := range pr.Topics {
				st := kmsg.NewProduceResponseTopic()
				st.Topic, st.TopicID = rt.Topic, rt.TopicID
				for _, rp := range rt.Partitions {
					sp := kmsg.NewProduceResponseTopicPartition()
					sp.Partition = rp.Partition
					sp.ErrorCode = kerr.NotEnoughReplicas.Code
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
			}
			return resp, nil, true
		}
		return nil, nil, false
	})

	cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
		return errBoom
	})
	cfg.MaxAttempts = 1
	cfg.DLQTopic = dlqTopic

	r := start(t, ctx, cfg)
	r.until(t, "source drained and committed", func() bool {
		return e.committed(t)[0] == 1
	})
	r.cancel()
	if err := r.wait(t, 15*time.Second); err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}

	dlqRecs := testenv.Records(t, brokers, dlqTopic)
	if len(dlqRecs) != 1 {
		t.Errorf("DLQ has %d records, want exactly 1", len(dlqRecs))
	}
	if n := dlqAttempts.Load(); n < 3 {
		t.Errorf("DLQ produce attempts = %d, want at least 3", n)
	}
}

// Case 6: DLQ produce fails with a non-retriable error.
// docs/CONTRACT.md Guarantee 3: if DLQ produce fails, Run returns error, commits nothing at or past it.
// Review fix: Uses errors.Is(err, kerr.TopicAuthorizationFailed) and verifies fast exit.
func TestEdgeDLQProduceNonRetriable(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects non-retriable Produce error with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	dlqTopic := testenv.Topic(t, brokers, 1)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	topics, err := kadm.NewClient(cl).ListTopics(ctx, dlqTopic)
	if err != nil {
		t.Fatal(err)
	}
	dlqID := topics[dlqTopic].ID

	e.produce(t, "r", 3)

	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		pr := req.(*kmsg.ProduceRequest)
		isDLQ := false
		for _, rt := range pr.Topics {
			if rt.Topic == dlqTopic || (dlqID != [16]byte{} && rt.TopicID == dlqID) {
				isDLQ = true
				break
			}
		}
		if !isDLQ {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range pr.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
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
	cfg.MaxAttempts = 1
	cfg.DLQTopic = dlqTopic

	r := start(t, ctx, cfg)
	err = r.wait(t, 5*time.Second) // fast return well under dlqTimeout (30s)
	if err == nil {
		t.Fatal("expected error from failed DLQ produce, got nil")
	}
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Errorf("expected error wrapping TopicAuthorizationFailed, got %v", err)
	}
	mustContain(t, "Run error", err.Error(), "TOPIC_AUTHORIZATION_FAILED")

	committed := e.committed(t)
	if committed[0] != 1 {
		t.Errorf("committed = %d, want 1 (offset 0 settled, 1 failed DLQ)", committed[0])
	}
}

// Case 7: OffsetCommit returns a retriable error once, then succeeds.
// docs/CONTRACT.md Guarantee 4: Commit.
// Review fix: After observing recovered commit 3, produce a second wave of records
// and require its handler identity and checkpoint (commit 5) to prove continued consumption.
func TestEdgeOffsetCommitRetriableOnce(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects OffsetCommit error with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "w1", 3)

	var commitAttempts atomic.Int32
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if commitAttempts.Add(1) == 1 {
			resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
			for _, rt := range req.(*kmsg.OffsetCommitRequest).Topics {
				st := kmsg.NewOffsetCommitResponseTopic()
				st.Topic = rt.Topic
				for _, rp := range rt.Partitions {
					sp := kmsg.NewOffsetCommitResponseTopicPartition()
					sp.Partition = rp.Partition
					sp.ErrorCode = kerr.CoordinatorNotAvailable.Code // retriable
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
			}
			return resp, nil, true
		}
		return nil, nil, false
	})

	var (
		mu          sync.Mutex
		seenOffsets []int64
	)
	r := start(t, ctx, e.cfg(func(_ context.Context, rec *kgo.Record) error {
		mu.Lock()
		seenOffsets = append(seenOffsets, rec.Offset)
		mu.Unlock()
		return nil
	}))

	// Wave 1 drained despite transient retriable commit error
	r.until(t, "wave 1 committed", func() bool {
		return e.committed(t)[0] == 3
	})

	// Wave 2: produce 2 more records to verify consumption genuinely continues
	e.produce(t, "w2", 2)
	r.until(t, "wave 2 committed", func() bool {
		return e.committed(t)[0] == 5
	})

	r.cancel()
	if err := r.wait(t, 5*time.Second); err != nil {
		t.Fatalf("Run returned %v after cancel, want nil", err)
	}

	mu.Lock()
	offsets := append([]int64(nil), seenOffsets...)
	mu.Unlock()

	expectedOffsets := []int64{0, 1, 2, 3, 4}
	if len(offsets) != len(expectedOffsets) {
		t.Errorf("seen offsets = %v, want %v", offsets, expectedOffsets)
	}
	if got := e.committed(t)[0]; got != 5 {
		t.Errorf("final committed = %d, want 5", got)
	}
	if n := commitAttempts.Load(); n < 2 {
		t.Errorf("commit attempts = %d, want >= 2", n)
	}
}

// Case 8: commit timeout path (ControlKey delays OffsetCommit > 10s).
// docs/CONTRACT.md Guarantee 4 & offsetkeeper.go commitTimeout const (10s):
// Review fix: Uses SleepControl with request-seen and cleanup release gates;
// keeps response withheld until Run reports the commit deadline.
func TestEdgeCommitTimeout(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("delays OffsetCommit with kfake's SleepControl; a real broker cannot be delayed")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 2)

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	reqSeen := make(chan struct{}, 1)
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		select {
		case reqSeen <- struct{}{}:
		default:
		}
		cluster.SleepControl(func() {
			<-release
		})
		return nil, nil, false
	})

	r := start(t, ctx, e.cfg(func(_ context.Context, _ *kgo.Record) error {
		return nil
	}))

	select {
	case <-reqSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("OffsetCommit request never arrived at broker")
	}

	err := r.wait(t, 20*time.Second)
	select {
	case <-release:
	default:
		close(release)
	}

	if err == nil {
		t.Fatal("expected error on commit timeout, got nil")
	}
	mustContain(t, "Run error", err.Error(), "offsetkeeper: commit:")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v should wrap context.DeadlineExceeded", err)
	}
}

// A broker that goes silent (black hole) must not stall shutdown: Run returns
// within commitTimeout + leaveTimeout (+ about 2s of kgo's own Close waits,
// client.go:1425 and :1447) of the cancel, not after kgo's unbounded
// LeaveGroup retries (52-72s measured).
func TestRun_BrokerBlackholeBoundedShutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("takes about commitTimeout + leaveTimeout")
	}
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("black-holes the broker with kfake's Control; a real broker cannot be told to")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 1)

	// From the first OffsetCommit on, every request goes unanswered.
	var silent atomic.Bool
	seen := make(chan struct{}, 1)
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		silent.Store(true)
		select {
		case seen <- struct{}{}:
		default:
		}
		return nil, nil, true
	})
	cluster.Control(func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		return nil, nil, silent.Load()
	})

	r := start(t, ctx, e.cfg(func(context.Context, *kgo.Record) error { return nil }))
	select {
	case <-seen:
	case <-time.After(10 * time.Second):
		t.Fatal("OffsetCommit never reached the broker")
	}
	t0 := time.Now()
	r.cancel()
	err := r.wait(t, 2*(commitTimeout+leaveTimeout)+30*time.Second)
	d := time.Since(t0)
	t.Logf("Run returned %v after the cancel; err=%v", d.Round(100*time.Millisecond), err)
	if limit := commitTimeout + leaveTimeout + 3*time.Second; d > limit {
		t.Errorf("Run took %v after cancel, want <= %v", d, limit)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error %v should wrap context.DeadlineExceeded", err)
	}
}

// The bounded leave still happens on a healthy broker: the member is gone from
// the group as soon as Run returns, not after the session timeout.
func TestRunLeavesGroupOnShutdown(t *testing.T) {
	brokers, ctx := testenv.Brokers(t), testCtx(t)
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 1)
	var handled atomic.Bool
	r := start(t, ctx, e.cfg(func(context.Context, *kgo.Record) error { handled.Store(true); return nil }))
	r.until(t, "the record to be handled", handled.Load)
	r.cancel()
	if err := r.wait(t, 20*time.Second); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	gs, err := kadm.NewClient(cl).DescribeGroups(ctx, e.group)
	if err != nil || gs[e.group].Err != nil {
		t.Fatalf("describe group: %v / %v", err, gs[e.group].Err)
	}
	if n := len(gs[e.group].Members); n != 0 {
		t.Errorf("group has %d members after Run returned, want 0", n)
	}
}

// Case 9a: Publisher: Publish with zero records.
// docs/CONTRACT.md Guarantee 8 & doc comment for Publish:
// Publish with no records returns nil without error or panic.
// Review fix: Retains one empty call.
func TestEdgePublisherZeroRecords(t *testing.T) {
	brokers := testenv.Brokers(t)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if err := p.Publish(t.Context()); err != nil {
		t.Errorf("Publish() = %v, want nil", err)
	}
}

// Case 9b: Publisher: with nil record in the slice (must not panic: or document).
// docs/CONTRACT.md and doc comment are silent on nil records in Publish slice.
// Safe behaviour requires: no panic, return a clear error.
// Known library bug (being fixed separately): Publisher.Publish panics on nil record in slice.
func TestEdgePublisherNilRecord(t *testing.T) {
	brokers := testenv.Brokers(t)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("known bug: Publisher.Publish panicked with nil record: %v (docs/CONTRACT.md/doc silent; safe behaviour requires error, no panic)", r)
		}
	}()
	_ = p.Publish(t.Context(), (*kgo.Record)(nil))
}

// Case 9c: Publisher: to a missing topic with UnknownTopicRetries default.
// docs/CONTRACT.md Guarantee 8 & doc comment for Publish:
// Review fix: Requires errors.Is(err, kerr.UnknownTopicOrPartition), labeled as default-client characterization.
func TestEdgePublisherMissingTopicDefaultRetries(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	mctx, cancel := context.WithTimeout(testCtx(t), 10*time.Second)
	defer cancel()

	err = p.Publish(mctx, &kgo.Record{Topic: topic + "-nonexistent", Value: []byte("val")})
	if err == nil {
		t.Error("Publish to missing topic returned nil, want error")
	} else {
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Errorf("Publish error = %v, want errors.Is UnknownTopicOrPartition", err)
		}
		if !strings.HasPrefix(err.Error(), "offsetkeeper: publish: ") {
			t.Errorf("Publish error = %q, want 'offsetkeeper: publish: ' prefix", err)
		}
	}
}

// Case 9d: Publisher: ctx cancelled before Publish.
// docs/CONTRACT.md Guarantee 8 & doc comment:
// Review fix: Uses testenv.Brokers(t) to honor backend selection.
func TestEdgePublisherContextCancelled(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	cctx, cancel := context.WithCancel(testCtx(t))
	cancel()

	err = p.Publish(cctx, &kgo.Record{Topic: topic, Value: []byte("val")})
	if err == nil {
		t.Error("Publish with cancelled context returned nil, want error")
	} else {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Publish error = %v, want context.Canceled", err)
		}
		if !strings.HasPrefix(err.Error(), "offsetkeeper: publish: ") {
			t.Errorf("Publish error = %q, want 'offsetkeeper: publish: ' prefix", err)
		}
	}
}

// Case 9e: Publisher: Close twice.
// Safe behaviour: Close releases resources and can be called multiple times without panic.
// Characterization test pending API documentation.
func TestEdgePublisherCloseTwice(t *testing.T) {
	brokers := testenv.Brokers(t)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	p.Close()
}

// Case 9f: Publisher: Publish after Close (error, no panic).
// docs/CONTRACT.md Guarantee 8 & doc comment:
// Review fix: Requires errors.Is(err, kgo.ErrClientClosed).
func TestEdgePublisherPublishAfterClose(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	p, err := NewPublisher(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	p.Close()

	err = p.Publish(testCtx(t), &kgo.Record{Topic: topic, Value: []byte("val")})
	if err == nil {
		t.Error("Publish after Close returned nil, want error")
	} else {
		if !errors.Is(err, kgo.ErrClientClosed) {
			t.Errorf("Publish error = %v, want errors.Is kgo.ErrClientClosed", err)
		}
		if !strings.HasPrefix(err.Error(), "offsetkeeper: publish: ") {
			t.Errorf("Publish error = %q, want 'offsetkeeper: publish: ' prefix", err)
		}
	}
}

// Missing Case (a): Publish a mixed batch with one accepted and one broker-rejected record.
// docs/CONTRACT.md Guarantee 8: Publish awaits all results, wraps the failure, and the accepted record is readable.
func TestEdgePublisherMixedBatchAcceptedAndRejected(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects targeted partition fault with kfake; a real broker cannot be told to fail a partition")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	topic := testenv.Topic(t, brokers, 2)

	p, err := NewPublisher(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Inject non-retriable failure on partition 1 only:
	cluster.Fault(kfake.Fault{
		Keys:       []kmsg.Key{kmsg.Produce},
		Topic:      topic,
		Partitions: []int32{1},
		Err:        kerr.TopicAuthorizationFailed,
		Count:      -1,
	})

	r0 := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("accepted-data")}
	r1 := &kgo.Record{Topic: topic, Partition: 1, Value: []byte("rejected-data")}

	err = p.Publish(ctx, r0, r1)
	if err == nil {
		t.Fatal("Publish returned nil, want error from rejected partition 1")
	}
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Errorf("Publish error %v does not wrap TopicAuthorizationFailed", err)
	}

	// Accepted record in partition 0 must be durable and readable
	recs := testenv.Records(t, brokers, topic)
	if len(recs) != 1 {
		t.Fatalf("topic has %d records, want exactly 1 (accepted partition 0)", len(recs))
	}
	if recs[0].Partition != 0 || string(recs[0].Value) != "accepted-data" {
		t.Errorf("got record partition %d val %q, want partition 0 val 'accepted-data'", recs[0].Partition, string(recs[0].Value))
	}
}

// Missing Case (b): Cancel while a settled-prefix commit is pending and inject non-retriable OffsetCommit failure.
// docs/CONTRACT.md Guarantees 4 & 6: The commit error wins over clean shutdown, and actual group offsets reflect failure.
func TestEdgeCommitErrorWinsOverShutdown(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("injects OffsetCommit error with kfake's ControlKey; a real broker cannot be told to fail")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
	e.produce(t, "r", 3)

	runCtx, cancel := context.WithCancel(ctx)

	var intercepted atomic.Int32
	cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		intercepted.Add(1)
		// Cancel the run context while OffsetCommit is pending
		cancel()

		resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
		for _, rt := range req.(*kmsg.OffsetCommitRequest).Topics {
			st := kmsg.NewOffsetCommitResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.GroupAuthorizationFailed.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	var handled atomic.Int32
	cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
		handled.Add(1)
		return nil
	})

	r := start(t, runCtx, cfg)
	err := r.wait(t, 10*time.Second)

	// 1. Commit error must win over clean shutdown (err is non-nil)
	if err == nil {
		t.Fatal("Run returned nil, want commit error to win over clean shutdown")
	}
	if !errors.Is(err, kerr.GroupAuthorizationFailed) {
		t.Errorf("expected GroupAuthorizationFailed, got %v", err)
	}
	mustContain(t, "Run error", err.Error(), "offsetkeeper: commit:")

	// 2. Check actual group offsets: nothing committed
	committed := e.committed(t)
	if len(committed) != 0 && committed[0] > 0 {
		t.Errorf("committed offset = %v, want nothing committed", committed)
	}
}

// ---- helpers ----

var errBoom = errors.New("boom")

type env struct {
	brokers      []string
	topic, group string
	partitions   int32
}

func (e env) cfg(h Handler) Config {
	return Config{
		Brokers: e.brokers, Group: e.group, Topics: []string{e.topic}, Handler: h,
		MaxAttempts: 3, Backoff: time.Millisecond,
	}
}

func (e env) produce(t *testing.T, prefix string, n int) {
	t.Helper()
	var rs []*kgo.Record
	for i := 0; i < n; i++ {
		rs = append(rs, &kgo.Record{
			Topic: e.topic, Partition: 0,
			Key: fmt.Appendf(nil, "k%d", i), Value: fmt.Appendf(nil, "%s-0-%d", prefix, i),
		})
	}
	testenv.Produce(t, e.brokers, rs...)
}

func (e env) committed(t *testing.T) map[int32]int64 {
	t.Helper()
	return testenv.Committed(t, e.brokers, e.group, e.topic)
}

type runner struct {
	cancel context.CancelFunc
	exited chan struct{}
	err    error
}

func start(t *testing.T, ctx context.Context, cfg Config) *runner {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	r := &runner{cancel: cancel, exited: make(chan struct{})}
	go func() { r.err = Run(ctx, cfg); close(r.exited) }()
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

func (r *runner) until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testenv.Eventually(t, 30*time.Second, func() bool {
		select {
		case <-r.exited:
			t.Fatalf("waiting for %s: Run exited early: %v", what, r.err)
		default:
		}
		return cond()
	}, what)
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

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
