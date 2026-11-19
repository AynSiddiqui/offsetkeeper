package offsetkeeper_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

// Case 1: Empty strings inside Brokers and Topics.
// Tests that fail only because of known library bugs (empty broker strings) are
// left as findings and marked in the report.
func TestEdge_EmptyStringBrokersAndTopics(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	// Subtest 1: Brokers with empty string element (alone and mixed)
	t.Run("empty string in Brokers slice alone", func(t *testing.T) {
		cfg := e.cfg(func(ctx context.Context, r *kgo.Record) error { return nil })
		cfg.Brokers = []string{""}
		runCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		err := offsetkeeper.Run(runCtx, cfg)
		// Expected: boundary validation rejection naming Config.Brokers and bad value "".
		// Actual: withDefaults only checks len == 0, dials :9092 and returns nil on ctx cancel.
		if err == nil || !strings.Contains(err.Error(), "Config.Brokers") {
			t.Errorf("Run with Brokers=[]string{\"\"} must return validation error naming Config.Brokers, got %v", err)
		}
	})

	t.Run("empty string among valid brokers", func(t *testing.T) {
		cfg := e.cfg(func(ctx context.Context, r *kgo.Record) error { return nil })
		cfg.Brokers = []string{e.brokers[0], ""}
		runCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		err := offsetkeeper.Run(runCtx, cfg)
		if err == nil || !strings.Contains(err.Error(), "Config.Brokers") {
			t.Errorf("Run with Brokers=[valid, \"\"] must return validation error naming Config.Brokers, got %v", err)
		}
	})

	// Subtest 2: Topics with empty string element (alone and mixed)
	t.Run("empty string in Topics slice alone", func(t *testing.T) {
		cfg := e.cfg(func(ctx context.Context, r *kgo.Record) error { return nil })
		cfg.Topics = []string{""}
		err := offsetkeeper.Run(ctx, cfg)
		if err == nil {
			t.Errorf("Run with Topics=[]string{\"\"} must return error, got nil")
		}
	})

	t.Run("empty string among valid topics", func(t *testing.T) {
		cfg := e.cfg(func(ctx context.Context, r *kgo.Record) error { return nil })
		cfg.Topics = []string{e.topic, ""}
		runCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		err := offsetkeeper.Run(runCtx, cfg)
		if err == nil {
			t.Errorf("Run with Topics=[valid, \"\"] must return error, got nil")
		}
	})
}

// Case 2: Duplicate topics in Config.Topics.
// Asserts the set of unique offsets {0, 1, 2} and final checkpoint 3.
func TestEdge_DuplicateTopics(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var mu sync.Mutex
	seenOffsets := make(map[int64]int)

	h := func(ctx context.Context, r *kgo.Record) error {
		mu.Lock()
		seenOffsets[r.Offset]++
		mu.Unlock()
		return nil
	}

	cfg := e.cfg(h)
	cfg.Topics = []string{e.topic, e.topic} // duplicate topic

	e.produce(t, "dup", 3) // offsets 0, 1, 2

	run := start(t, ctx, cfg)
	run.until(t, "all 3 distinct offsets handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seenOffsets) == 3
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	mu.Lock()
	for _, o := range []int64{0, 1, 2} {
		if seenOffsets[o] == 0 {
			t.Errorf("offset %d was not handled", o)
		}
	}
	t.Logf("healthy-run multiplicity: %+v", seenOffsets)
	mu.Unlock()

	// Assert final checkpoint
	comm := e.committed(t)
	if comm[0] != 3 {
		t.Errorf("final checkpoint = %d, want 3", comm[0])
	}
}

// Case 3: Group with spaces and unicode.
// Suffixes testenv.Group(t) with unicode text and asserts offset 2 on that exact group.
func TestEdge_GroupSpacesAndUnicode(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var handled atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		handled.Add(1)
		return nil
	}

	cfg := e.cfg(h)
	cfg.Group = testenv.Group(t) + " with spaces and 🚀 unicode"

	e.produce(t, "uni", 2)

	run := start(t, ctx, cfg)
	run.until(t, "2 records handled", func() bool {
		return handled.Load() >= 2
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if got := handled.Load(); got != 2 {
		t.Errorf("handled %d records, want 2", got)
	}

	// Verify committed offset for the exact Unicode group
	comm := testenv.Committed(t, e.brokers, cfg.Group, e.topic)
	if comm[0] != 2 {
		t.Errorf("committed offset for group %q = %d, want 2", cfg.Group, comm[0])
	}
}

// Case 4: MaxAttempts huge (1e9) with tiny Backoff (bounded by ctx cancellation).
func TestEdge_HugeMaxAttemptsWithTinyBackoff(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var attempts atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		attempts.Add(1)
		return errBoom
	}

	cfg := e.cfg(h)
	cfg.MaxAttempts = 1_000_000_000
	cfg.Backoff = time.Nanosecond

	e.produce(t, "huge", 1)

	run := start(t, ctx, cfg)
	run.until(t, "retried at least 5 times", func() bool {
		return attempts.Load() >= 5
	})
	run.cancel()
	err := run.wait(t, 5*time.Second)
	if err != nil {
		t.Errorf("expected clean shutdown nil error on cancel, got: %v", err)
	}
}

// Case 7a: ClientOpts with kgo.ConsumerGroup("other") -> offsetkeeper's group wins.
func TestEdge_ClientOptsConsumerGroupOverride(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	otherGroup := testenv.Group(t)
	var handled atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		handled.Add(1)
		return nil
	}

	cfg := e.cfg(h)
	cfg.ClientOpts = []kgo.Opt{kgo.ConsumerGroup(otherGroup)}

	e.produce(t, "cg-override", 1)

	run := start(t, ctx, cfg)
	run.until(t, "record handled", func() bool {
		return handled.Load() >= 1
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify offsetkeeper's group committed the offset
	comm := e.committed(t)
	if comm[0] != 1 {
		t.Errorf("expected cfg.Group %q to commit offset 1, got %d", e.group, comm[0])
	}
	// Verify otherGroup has NO commits
	otherComm := testenv.Committed(t, e.brokers, otherGroup, e.topic)
	if at, ok := otherComm[0]; ok {
		t.Errorf("expected otherGroup %q to have no commit, got %d", otherGroup, at)
	}
}

// Case 7b: ClientOpts with kgo.ConsumeTopics("other") -> offsetkeeper's topics win.
// Uses a fresh main/extra pair, seeds a main sentinel and checks that extra topic
// is not consumed.
func TestEdge_ClientOptsConsumeTopicsOverride(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	mainTopic := testenv.Topic(t, e.brokers, 1)
	extraTopic := testenv.Topic(t, e.brokers, 1)

	// Produce distinct records to both topics
	testenv.Produce(t, e.brokers,
		&kgo.Record{Topic: extraTopic, Partition: 0, Key: []byte("extra-k"), Value: []byte("extra-v")},
		&kgo.Record{Topic: mainTopic, Partition: 0, Key: []byte("main-k"), Value: []byte("main-v")},
	)

	var extraSeen atomic.Bool
	var mainSeen atomic.Bool

	h := func(ctx context.Context, r *kgo.Record) error {
		if r.Topic == extraTopic {
			extraSeen.Store(true)
		}
		if r.Topic == mainTopic {
			mainSeen.Store(true)
		}
		return nil
	}

	cfg := offsetkeeper.Config{
		Brokers:     e.brokers,
		Group:       testenv.Group(t),
		Topics:      []string{mainTopic},
		Handler:     h,
		ClientOpts:  []kgo.Opt{kgo.ConsumeTopics(extraTopic)},
		MaxAttempts: 1,
	}

	run := start(t, ctx, cfg)
	run.until(t, "main topic sentinel handled", func() bool {
		return mainSeen.Load()
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if extraSeen.Load() {
		t.Errorf("records from ClientOpts extraTopic %q were consumed; offsetkeeper's configured Topics must win", extraTopic)
	}

	comm := testenv.Committed(t, e.brokers, cfg.Group, mainTopic)
	if comm[0] != 1 {
		t.Errorf("mainTopic committed offset = %d, want 1", comm[0])
	}
	extraComm := testenv.Committed(t, e.brokers, cfg.Group, extraTopic)
	if at, ok := extraComm[0]; ok {
		t.Errorf("extraTopic should not be committed, got %d", at)
	}
}

// Case 7c: ClientOpts with auto-commit options -> offsetkeeper's DisableAutoCommit wins.
// Uses valid AutoCommitInterval alone (>=100ms), requires errors.Is(err, errBoom),
// and verifies no commit past unsettled record.
func TestEdge_ClientOptsAutoCommitOverride(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var handlerCalls atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		handlerCalls.Add(1)
		return errBoom // fail immediately without settling
	}

	cfg := e.cfg(h)
	// Valid AutoCommitInterval alone (1s >= 100ms minimum)
	cfg.ClientOpts = []kgo.Opt{
		kgo.AutoCommitInterval(1 * time.Second),
	}
	cfg.MaxAttempts = 1
	cfg.DLQTopic = "" // No DLQ: Run will return error on failure

	e.produce(t, "autocommit", 3)

	run := start(t, ctx, cfg)
	err := run.wait(t, waitFor)
	if err == nil {
		t.Fatal("expected Run to fail on handler error, got nil")
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("expected errBoom, got: %v", err)
	}
	if handlerCalls.Load() == 0 {
		t.Errorf("expected handler to be called, got 0 calls")
	}

	// Guarantee 4: Auto-commit is off. Never past an unsettled record.
	comm := e.committed(t)
	if at, ok := comm[0]; ok {
		t.Errorf("expected no commit past unsettled record (absent), got offset %d", at)
	}
}

// Case 7d: ClientOpts with kgo.ConsumeRegex() using an anchored pattern matching
// two fresh topics, producing distinct sentinels to both, and verifying identities.
func TestEdge_ClientOptsConsumeRegex(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	base := testenv.Topic(t, e.brokers, 1)
	topicA := base + "-alpha"
	topicB := base + "-beta"

	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)
	tctx, tcancel := context.WithTimeout(ctx, 10*time.Second)
	defer tcancel()
	if _, err := adm.CreateTopics(tctx, 1, 1, nil, topicA, topicB); err != nil {
		t.Fatalf("create topics: %v", err)
	}

	testenv.Produce(t, e.brokers,
		&kgo.Record{Topic: topicA, Partition: 0, Key: []byte("k-a"), Value: []byte("val-a")},
		&kgo.Record{Topic: topicB, Partition: 0, Key: []byte("k-b"), Value: []byte("val-b")},
	)

	var seenA, seenB atomic.Bool
	h := func(ctx context.Context, r *kgo.Record) error {
		if r.Topic == topicA && string(r.Value) == "val-a" {
			seenA.Store(true)
		}
		if r.Topic == topicB && string(r.Value) == "val-b" {
			seenB.Store(true)
		}
		return nil
	}

	cfg := offsetkeeper.Config{
		Brokers:     e.brokers,
		Group:       testenv.Group(t),
		Topics:      []string{"^" + regexp.QuoteMeta(base) + "-(alpha|beta)$"},
		Handler:     h,
		ClientOpts:  []kgo.Opt{kgo.ConsumeRegex()},
		MaxAttempts: 1,
	}

	run := start(t, ctx, cfg)
	run.until(t, "both regex matched topics handled", func() bool {
		return seenA.Load() && seenB.Load()
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed with ConsumeRegex: %v", err)
	}
}

// Case 8: ClientOpts with kgo.SeedBrokers pointing elsewhere.
func TestEdge_ClientOptsSeedBrokersElsewhere(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var handled atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		handled.Add(1)
		return nil
	}

	cfg := e.cfg(h)
	cfg.ClientOpts = []kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}

	e.produce(t, "seed-override", 2)

	run := start(t, ctx, cfg)
	run.until(t, "records handled despite bad seed in ClientOpts", func() bool {
		return handled.Load() >= 2
	})
	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if got := handled.Load(); got != 2 {
		t.Errorf("handled %d records, want 2", got)
	}
}

// Cases 9, 10, 11: Table test for Permanent error classification and unwrapping.
func TestEdge_PermanentErrorClassification(t *testing.T) {
	otherErr := errors.New("other transient error")

	tests := []struct {
		name       string
		errFn      func() error
		checkCause func(t *testing.T, err error)
	}{
		{
			name: "nested Permanent",
			errFn: func() error {
				return offsetkeeper.Permanent(offsetkeeper.Permanent(errBoom))
			},
			checkCause: func(t *testing.T, err error) {
				if !errors.Is(err, errBoom) {
					t.Errorf("errors.Is(err, errBoom) failed: %v", err)
				}
			},
		},
		{
			name: "wrapped by fmt.Errorf %w",
			errFn: func() error {
				return fmt.Errorf("outer context: %w", offsetkeeper.Permanent(errBoom))
			},
			checkCause: func(t *testing.T, err error) {
				if !errors.Is(err, errBoom) {
					t.Errorf("errors.Is(err, errBoom) failed: %v", err)
				}
			},
		},
		{
			name: "errors.Join containing Permanent",
			errFn: func() error {
				return errors.Join(otherErr, offsetkeeper.Permanent(errBoom))
			},
			checkCause: func(t *testing.T, err error) {
				if !errors.Is(err, errBoom) {
					t.Errorf("errors.Is(err, errBoom) failed: %v", err)
				}
				if !errors.Is(err, otherErr) {
					t.Errorf("errors.Is(err, otherErr) failed: %v", err)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx(t)
			e := newEnv(t, 1)

			var attempts atomic.Int32
			h := func(ctx context.Context, r *kgo.Record) error {
				attempts.Add(1)
				return tc.errFn()
			}

			cfg := e.cfg(h)
			cfg.MaxAttempts = 5
			cfg.DLQTopic = ""

			e.produce(t, "perm", 1)

			run := start(t, ctx, cfg)
			err := run.wait(t, waitFor)
			if err == nil {
				t.Fatal("expected Run to fail, got nil")
			}
			tc.checkCause(t, err)
			if got := attempts.Load(); got != 1 {
				t.Errorf("attempts = %d, want 1 (retries should be skipped)", got)
			}
		})
	}
}

// Case 12: Handler returning context.Canceled / DeadlineExceeded while Run's ctx is live.
func TestEdge_HandlerContextCanceledWhileRunLive(t *testing.T) {
	for _, testErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(testErr.Error(), func(t *testing.T) {
			ctx := testCtx(t)
			e := newEnv(t, 1)

			dlqTopic := testenv.Topic(t, e.brokers, 1)

			var attempts atomic.Int32
			h := func(ctx context.Context, r *kgo.Record) error {
				attempts.Add(1)
				return testErr
			}

			cfg := e.cfg(h)
			cfg.MaxAttempts = 3
			cfg.Backoff = time.Millisecond
			cfg.DLQTopic = dlqTopic

			e.produce(t, "ctx-err", 1)

			run := start(t, ctx, cfg)
			run.until(t, "3 attempts executed and settled via DLQ", func() bool {
				return attempts.Load() >= 3
			})

			testenv.Eventually(t, waitFor, func() bool {
				comm := e.committed(t)
				return comm[0] == 1
			}, "source topic offset to commit at 1 after DLQ publish")

			run.cancel()
			if err := run.wait(t, waitFor); err != nil {
				t.Fatalf("Run failed: %v", err)
			}

			if got := attempts.Load(); got != 3 {
				t.Errorf("attempts = %d, want 3", got)
			}

			if got := e.total(t, dlqTopic); got != 1 {
				t.Errorf("DLQ has %d records, want 1", got)
			}
		})
	}
}

type typedNilError struct{}

func (*typedNilError) Error() string {
	return "typed nil error"
}

// Case 13: Handler returning typed nil error pointer.
// Tests both no-DLQ (error returned, exact attempts, cause via errors.As, no commit advancement)
// and DLQ-enabled (settled via DLQ, attempt header verified).
func TestEdge_HandlerTypedNilError(t *testing.T) {
	t.Run("no DLQ: exact attempts, errors.As match, no commit", func(t *testing.T) {
		ctx := testCtx(t)
		e := newEnv(t, 1)

		var attempts atomic.Int32
		h := func(ctx context.Context, r *kgo.Record) error {
			attempts.Add(1)
			var nilErr *typedNilError = nil
			return nilErr
		}

		cfg := e.cfg(h)
		cfg.MaxAttempts = 3
		cfg.Backoff = time.Millisecond
		cfg.DLQTopic = ""

		e.produce(t, "typed-nil-nodlq", 1)

		run := start(t, ctx, cfg)
		err := run.wait(t, waitFor)
		if err == nil {
			t.Fatal("expected Run to fail, got nil")
		}

		var target *typedNilError
		if !errors.As(err, &target) {
			t.Errorf("errors.As(err, &target) failed to find typed nil error: %v", err)
		}

		if got := attempts.Load(); got != 3 {
			t.Errorf("attempts = %d, want 3", got)
		}

		comm := e.committed(t)
		if at, ok := comm[0]; ok {
			t.Errorf("expected no commit advancement, got offset %d", at)
		}
	})

	t.Run("with DLQ: settled via DLQ, headers verified", func(t *testing.T) {
		ctx := testCtx(t)
		e := newEnv(t, 1)
		dlqTopic := testenv.Topic(t, e.brokers, 1)

		var attempts atomic.Int32
		h := func(ctx context.Context, r *kgo.Record) error {
			attempts.Add(1)
			var nilErr *typedNilError = nil
			return nilErr
		}

		cfg := e.cfg(h)
		cfg.MaxAttempts = 2
		cfg.Backoff = time.Millisecond
		cfg.DLQTopic = dlqTopic

		e.produce(t, "typed-nil-dlq", 1)

		run := start(t, ctx, cfg)
		run.until(t, "attempts reach 2", func() bool {
			return attempts.Load() >= 2
		})

		testenv.Eventually(t, waitFor, func() bool {
			comm := e.committed(t)
			return comm[0] == 1
		}, "offset 1 committed after DLQ settlement")

		run.cancel()
		if err := run.wait(t, waitFor); err != nil {
			t.Fatalf("Run failed: %v", err)
		}

		if got := attempts.Load(); got != 2 {
			t.Errorf("attempts = %d, want 2", got)
		}
		if got := e.total(t, dlqTopic); got != 1 {
			t.Fatalf("DLQ count = %d, want 1", got)
		}
	})
}

func tryGroupState(brokers []string, group string) (state string, members int, ok bool) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return "", 0, false
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	gs, err := kadm.NewClient(cl).DescribeGroups(ctx, group)
	if err != nil || gs[group].Err != nil {
		return "", 0, false
	}
	return gs[group].State, len(gs[group].Members), true
}

// Case 14: Run called twice concurrently with the same group and same Config.
// Uses two cleanup-owned runners, waits for a stable 2-member assignment before
// producing, checks partition identities and both clean returns.
func TestEdge_RunConcurrentSameGroupSameConfig(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 2) // 2 partitions

	var (
		mu         sync.Mutex
		partsSeen  = make(map[int32]int)
		totalCalls atomic.Int32
	)

	h := func(ctx context.Context, r *kgo.Record) error {
		mu.Lock()
		partsSeen[r.Partition]++
		mu.Unlock()
		totalCalls.Add(1)
		return nil
	}

	cfg := e.cfg(h)

	run1 := start(t, ctx, cfg)
	run2 := start(t, ctx, cfg)

	// Wait for group to stabilize with 2 members
	testenv.Eventually(t, waitFor, func() bool {
		state, members, ok := tryGroupState(e.brokers, cfg.Group)
		return ok && members == 2 && state == "Stable"
	}, "group to reach 2 members and Stable state")

	// Produce 2 records per partition = 4 total
	e.produce(t, "concurrent", 2)

	run1.until(t, "all 4 records handled", func() bool {
		return totalCalls.Load() >= 4
	})

	testenv.Eventually(t, waitFor, func() bool {
		return e.drained(t)
	}, "both partitions to be drained and committed")

	run1.cancel()
	run2.cancel()

	err1 := run1.wait(t, waitFor)
	err2 := run2.wait(t, waitFor)

	if err1 != nil {
		t.Errorf("runner 1 exit error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("runner 2 exit error: %v", err2)
	}

	mu.Lock()
	if partsSeen[0] == 0 || partsSeen[1] == 0 {
		t.Errorf("expected records handled on both partitions, got %+v", partsSeen)
	}
	mu.Unlock()
}

// Case 16: Config reused after Run returns.
// Snapshots slice backing arrays and spare capacity, asserts exact offset sequences
// and checkpoints after both runs.
func TestEdge_ConfigReusedAfterRunReturns(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	var (
		mu      sync.Mutex
		run1Seq []int64
		run2Seq []int64
	)

	var currentRun atomic.Int32
	currentRun.Store(1)

	h := func(ctx context.Context, r *kgo.Record) error {
		mu.Lock()
		defer mu.Unlock()
		if currentRun.Load() == 1 {
			run1Seq = append(run1Seq, r.Offset)
		} else {
			run2Seq = append(run2Seq, r.Offset)
		}
		return nil
	}

	brokers := make([]string, 1, 5)
	brokers[0] = e.brokers[0]
	topics := make([]string, 1, 5)
	topics[0] = e.topic
	clientOpts := make([]kgo.Opt, 0, 5)

	cfg := offsetkeeper.Config{
		Brokers:     brokers,
		Group:       e.group,
		Topics:      topics,
		Handler:     h,
		ClientOpts:  clientOpts,
		MaxAttempts: 0,
		Backoff:     0,
		Logger:      nil,
	}

	// Capture initial slice headers
	initBrokersLen, initBrokersCap := len(cfg.Brokers), cap(cfg.Brokers)
	initTopicsLen, initTopicsCap := len(cfg.Topics), cap(cfg.Topics)
	initOptsLen, initOptsCap := len(cfg.ClientOpts), cap(cfg.ClientOpts)

	// Run 1: produce and handle offsets 0, 1
	e.produce(t, "batch1", 2)
	run1 := start(t, ctx, cfg)
	run1.until(t, "run1 handles 2 records", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(run1Seq) >= 2
	})
	run1.cancel()
	if err := run1.wait(t, waitFor); err != nil {
		t.Fatalf("first Run failed: %v", err)
	}

	comm1 := e.committed(t)
	if comm1[0] != 2 {
		t.Errorf("run1 checkpoint = %d, want 2", comm1[0])
	}

	// Verify slice backing storage and capacity were not mutated
	if len(cfg.Brokers) != initBrokersLen || cap(cfg.Brokers) != initBrokersCap ||
		len(cfg.Topics) != initTopicsLen || cap(cfg.Topics) != initTopicsCap ||
		len(cfg.ClientOpts) != initOptsLen || cap(cfg.ClientOpts) != initOptsCap {
		t.Errorf("cfg slices mutated after run1: brokers=%d/%d, topics=%d/%d, opts=%d/%d",
			len(cfg.Brokers), cap(cfg.Brokers), len(cfg.Topics), cap(cfg.Topics), len(cfg.ClientOpts), cap(cfg.ClientOpts))
	}

	// Run 2: switch flag, produce batch 2 (offsets 2, 3), reuse exact same cfg
	currentRun.Store(2)
	e.produce(t, "batch2", 2)
	run2 := start(t, ctx, cfg)
	run2.until(t, "run2 handles next 2 records", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(run2Seq) >= 2
	})
	run2.cancel()
	if err := run2.wait(t, waitFor); err != nil {
		t.Fatalf("second Run failed: %v", err)
	}

	comm2 := e.committed(t)
	if comm2[0] != 4 {
		t.Errorf("run2 checkpoint = %d, want 4", comm2[0])
	}

	mu.Lock()
	if !slices.Equal(run1Seq, []int64{0, 1}) {
		t.Errorf("run1 offsets = %v, want [0, 1]", run1Seq)
	}
	if !slices.Equal(run2Seq, []int64{2, 3}) {
		t.Errorf("run2 offsets = %v, want [2, 3]", run2Seq)
	}
	mu.Unlock()
}

// Case 18: Handler panic(nil) is recovered and retried exactly 3 times then DLQ'd.
// After Run joins, requires exactly 3 attempts, DLQ attempts header "3", and checkpoint 1.
func TestEdge_HandlerPanicNil(t *testing.T) {
	ctx := testCtx(t)
	e := newEnv(t, 1)

	dlqTopic := testenv.Topic(t, e.brokers, 1)

	var attempts atomic.Int32
	h := func(ctx context.Context, r *kgo.Record) error {
		attempts.Add(1)
		panic(nil) // panic with nil value
	}

	cfg := e.cfg(h)
	cfg.MaxAttempts = 3
	cfg.Backoff = time.Millisecond
	cfg.DLQTopic = dlqTopic

	e.produce(t, "panic-nil", 1)

	run := start(t, ctx, cfg)
	run.until(t, "DLQ record written and source committed", func() bool {
		comm := e.committed(t)
		return comm[0] == 1
	})

	run.cancel()
	if err := run.wait(t, waitFor); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// After Run joins, assert exact attempts
	if got := attempts.Load(); got != 3 {
		t.Errorf("exact attempts after join = %d, want 3", got)
	}

	if got := e.total(t, dlqTopic); got != 1 {
		t.Fatalf("DLQ total = %d, want 1", got)
	}

	// Verify DLQ headers
	dlqCl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumeTopics(dlqTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer dlqCl.Close()

	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	defer pcancel()
	fetches := dlqCl.PollRecords(pctx, 1)
	if fetches.IsClientClosed() || pctx.Err() != nil {
		t.Fatal("failed to poll DLQ record")
	}

	var foundRecord *kgo.Record
	fetches.EachRecord(func(r *kgo.Record) {
		foundRecord = r
	})
	if foundRecord == nil {
		t.Fatal("no DLQ record found")
	}

	attHeader, ok := header(foundRecord, "offsetkeeper.attempts")
	if !ok || attHeader != "3" {
		t.Errorf("DLQ attempts header = %q (ok=%v), want \"3\"", attHeader, ok)
	}
	errHeader, ok := header(foundRecord, "offsetkeeper.error")
	if !ok || !strings.Contains(errHeader, "panic") {
		t.Errorf("DLQ error header = %q (ok=%v), want containing \"panic\"", errHeader, ok)
	}
}
