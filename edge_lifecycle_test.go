package offsetkeeper_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/AynSiddiqui/offsetkeeper"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

// Case 1: 64 partitions with concurrent handlers.
// Asserts CONTRACT.md Guarantee 2 (order within a partition; partitions run concurrently)
// and Guarantee 4 (commits contiguous settled prefix per partition).
// Concurrency is proven even on GOMAXPROCS=1 by gating partition 0 until partition 1 enters.
func Test64PartitionsConcurrent(t *testing.T) {
	const numParts = 64
	const recsPerPart = 5
	const totalRecs = numParts * recsPerPart

	e, ctx := newEnv(t, numParts), testCtx(t)
	e.produce(t, "m", recsPerPart)

	var (
		partActive   [numParts]atomic.Int32
		partNextOff  [numParts]int64
		mu           sync.Mutex
		orderErrs    []string
		concurErrors []string

		p0Entered        = make(chan struct{})
		p0Once           sync.Once
		anySecondEntered = make(chan struct{})
		secondOnce       sync.Once
		peakConcur       atomic.Int32
		concurrent       atomic.Int32
		handled          atomic.Int32
	)
	p0Close := func() { p0Once.Do(func() { close(p0Entered) }) }
	secondClose := func() { secondOnce.Do(func() { close(anySecondEntered) }) }
	t.Cleanup(func() {
		p0Close()
		secondClose()
	})

	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		cur := concurrent.Add(1)
		for {
			old := peakConcur.Load()
			if cur <= old || peakConcur.CompareAndSwap(old, cur) {
				break
			}
		}
		defer concurrent.Add(-1)

		if r.Partition < 0 || r.Partition >= numParts {
			return fmt.Errorf("unexpected partition %d", r.Partition)
		}

		if inFlight := partActive[r.Partition].Add(1); inFlight > 1 {
			mu.Lock()
			concurErrors = append(concurErrors, fmt.Sprintf("partition %d had %d concurrent handlers", r.Partition, inFlight))
			mu.Unlock()
		}
		defer partActive[r.Partition].Add(-1)

		// Hold partition 0 until a second partition enters to guarantee concurrent overlap even on 1 CPU
		if r.Partition == 0 && r.Offset == 0 {
			p0Close()
			select {
			case <-anySecondEntered:
			case <-time.After(2 * time.Second):
			}
		} else {
			select {
			case <-p0Entered:
				secondClose()
			default:
			}
		}

		mu.Lock()
		expected := partNextOff[r.Partition]
		if r.Offset != expected {
			orderErrs = append(orderErrs, fmt.Sprintf("partition %d: got offset %d, want %d", r.Partition, r.Offset, expected))
		}
		partNextOff[r.Partition] = r.Offset + 1
		mu.Unlock()

		handled.Add(1)
		return nil
	})

	r := start(t, ctx, cfg)
	testenv.Eventually(t, 20*time.Second, func() bool {
		return handled.Load() == totalRecs
	}, fmt.Sprintf("all %d records handled", totalRecs))

	r.cancel()
	if err := r.wait(t, 10*time.Second); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(concurErrors) > 0 {
		t.Errorf("concurrent handlers within partition: %v", concurErrors)
	}
	if len(orderErrs) > 0 {
		t.Errorf("ordering violations: %v", orderErrs)
	}
	if peak := peakConcur.Load(); peak <= 1 {
		t.Errorf("peak concurrent handlers across partitions was %d, expected >= 2", peak)
	}

	testenv.Eventually(t, 10*time.Second, func() bool {
		return e.drained(t)
	}, "all 64 partitions committed to end")
}

// Case 2: Goroutine leak verification across all termination paths.
// Asserts STYLE.md Concurrency ("every go statement has one owner that waits for it")
// and CONTRACT.md Guarantee 6 (clean shutdown on exit).
// Uses stack inspection for offsetkeeper worker frames and validates return errors.
func TestGoroutineLeak(t *testing.T) {
	cases := []struct {
		name      string
		handler   offsetkeeper.Handler
		cancel    bool
		checkPath func(t *testing.T, err error)
	}{
		{
			name: "nil_path",
			handler: func(_ context.Context, _ *kgo.Record) error {
				return nil
			},
			cancel: true,
			checkPath: func(t *testing.T, err error) {
				if err != nil {
					t.Errorf("expected nil error on cancel, got %v", err)
				}
			},
		},
		{
			name: "error_path",
			handler: func(_ context.Context, _ *kgo.Record) error {
				return offsetkeeper.Permanent(errBoom)
			},
			checkPath: func(t *testing.T, err error) {
				if !errors.Is(err, errBoom) {
					t.Errorf("expected errBoom, got %v", err)
				}
			},
		},
		{
			name: "panic_path",
			handler: func(_ context.Context, _ *kgo.Record) error {
				panic("fatal panic")
			},
			checkPath: func(t *testing.T, err error) {
				if err == nil || !strings.Contains(err.Error(), "handler panic") {
					t.Errorf("expected handler panic error, got %v", err)
				}
			},
		},
		{
			name: "goexit_path",
			handler: func(_ context.Context, _ *kgo.Record) error {
				runtime.Goexit()
				return nil
			},
			checkPath: func(t *testing.T, err error) {
				if err == nil || !strings.Contains(err.Error(), "runtime.Goexit") {
					t.Errorf("expected runtime.Goexit error, got %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, ctx := newEnv(t, 2), testCtx(t)
			e.produce(t, "r", 2)

			// Record baseline goroutine count after GC
			var base int
			testenv.Eventually(t, 2*time.Second, func() bool {
				runtime.GC()
				base = runtime.NumGoroutine()
				return true
			})

			var (
				handled atomic.Int32
				invoked atomic.Bool
			)
			cfg := e.cfg(func(hctx context.Context, r *kgo.Record) error {
				invoked.Store(true)
				err := tc.handler(hctx, r)
				if err == nil {
					handled.Add(1)
				}
				return err
			})
			cfg.MaxAttempts = 1
			r := start(t, ctx, cfg)

			if tc.cancel {
				testenv.Eventually(t, 5*time.Second, func() bool {
					return handled.Load() == 4
				}, "all 4 records handled")
				r.cancel()
			}

			err := r.wait(t, 10*time.Second)
			if !invoked.Load() {
				t.Fatal("handler was never invoked")
			}
			tc.checkPath(t, err)

			// Poll until:
			// 1. Stack inspection confirms NO running goroutines inside offsetkeeper.(*worker) or offsetkeeper.Run
			// 2. NumGoroutine returns to baseline
			buf := make([]byte, 64*1024)
			testenv.Eventually(t, 10*time.Second, func() bool {
				runtime.GC()
				n := runtime.Stack(buf, true)
				stackStr := string(buf[:n])
				hasWorker := strings.Contains(stackStr, "offsetkeeper.(*worker)") ||
					strings.Contains(stackStr, "offsetkeeper.Run")
				return !hasWorker && runtime.NumGoroutine() <= base
			}, "goroutines to return to baseline with zero offsetkeeper worker stacks")
		})
	}
}

// Case 3: Cancel during poll, during handler, during backoff, during DLQ produce, during commit.
// Asserts CONTRACT.md Guarantee 6 (Shutdown), Guarantee 4 (Commit on error/exit), and
// STYLE.md Reliability rule 3 (every wait bounded, commits survive cancel using WithoutCancel).
func TestCancelDuringLifecycle(t *testing.T) {
	t.Run("poll", func(t *testing.T) {
		if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
			t.Skip("kfake fault injection / observation")
		}
		cluster, ctx := testenv.Kfake(t), testCtx(t)
		brokers := cluster.ListenAddrs()
		e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}

		pollStarted := make(chan struct{})
		var once atomic.Bool
		cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
			if once.CompareAndSwap(false, true) {
				close(pollStarted)
			}
			return nil, nil, false
		})

		cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
			return nil
		})
		r := start(t, ctx, cfg)
		select {
		case <-pollStarted:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for poll to begin")
		}
		r.cancel()
		err := r.wait(t, 2*time.Second)
		if err != nil {
			t.Fatalf("expected nil on cancel during poll, got: %v", err)
		}
	})

	t.Run("handler", func(t *testing.T) {
		e, ctx := newEnv(t, 1), testCtx(t)
		dlqTopic := testenv.Topic(t, e.brokers, 1)
		e.produce(t, "m", 2)
		started := make(chan struct{})
		cfg := e.cfg(func(hctx context.Context, _ *kgo.Record) error {
			close(started)
			<-hctx.Done()
			return hctx.Err()
		})
		cfg.DLQTopic = dlqTopic
		r := start(t, ctx, cfg)
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for handler to start")
		}
		r.cancel()
		err := r.wait(t, 2*time.Second)
		if err != nil {
			t.Fatalf("expected nil on cancel during handler, got: %v", err)
		}
		// Unsettled record must not be committed and not dead-lettered
		testenv.Eventually(t, 3*time.Second, func() bool {
			return e.committed(t)[0] == 0
		}, "unsettled record not committed")
		if dlqRecs := testenv.Records(t, e.brokers, dlqTopic); len(dlqRecs) != 0 {
			t.Errorf("expected 0 DLQ records on cancel, got %d", len(dlqRecs))
		}
	})

	t.Run("backoff", func(t *testing.T) {
		e, ctx := newEnv(t, 1), testCtx(t)
		e.produce(t, "m", 1)
		var attempts atomic.Int32
		inBackoff := make(chan struct{})
		cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
			if attempts.Add(1) == 1 {
				close(inBackoff)
				return errors.New("temporary error")
			}
			return nil
		})
		cfg.Backoff = 5 * time.Second
		cfg.MaxAttempts = 3
		r := start(t, ctx, cfg)
		select {
		case <-inBackoff:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for backoff to begin")
		}
		t0 := time.Now()
		r.cancel()
		err := r.wait(t, 2*time.Second)
		if err != nil {
			t.Fatalf("expected nil on cancel during backoff, got: %v", err)
		}
		if elapsed := time.Since(t0); elapsed >= 4*time.Second {
			t.Errorf("expected backoff to abort immediately, took %v", elapsed)
		}
		if att := attempts.Load(); att != 1 {
			t.Errorf("expected exactly 1 attempt before backoff abort, got %d", att)
		}
		testenv.Eventually(t, 3*time.Second, func() bool {
			return e.committed(t)[0] == 0
		}, "unsettled record not committed")
	})

	t.Run("dlq_produce", func(t *testing.T) {
		if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
			t.Skip("kfake fault injection")
		}
		cluster, ctx := testenv.Kfake(t), testCtx(t)
		brokers := cluster.ListenAddrs()
		e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
		dlqTopic := testenv.Topic(t, brokers, 1)
		e.produce(t, "m", 1)

		produceInFlight := make(chan struct{}, 1)
		unblockProduce := make(chan struct{})
		var (
			once        atomic.Bool
			unblockOnce sync.Once
		)
		t.Cleanup(func() {
			unblockOnce.Do(func() { close(unblockProduce) })
		})

		cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
			if once.CompareAndSwap(false, true) {
				select {
				case produceInFlight <- struct{}{}:
				default:
				}
				cluster.SleepControl(func() {
					<-unblockProduce
				})
			}
			return nil, nil, false
		})

		cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
			return offsetkeeper.Permanent(errBoom)
		})
		cfg.DLQTopic = dlqTopic
		r := start(t, ctx, cfg)

		select {
		case <-produceInFlight:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for DLQ produce in flight")
		}
		t0 := time.Now()
		r.cancel()
		unblockOnce.Do(func() { close(unblockProduce) })

		err := r.wait(t, 3*time.Second)
		if err != nil {
			t.Fatalf("expected nil on cancel during DLQ produce, got: %v", err)
		}
		if elapsed := time.Since(t0); elapsed >= 3*time.Second {
			t.Errorf("expected quick return on cancel, took %v", elapsed)
		}

		recs := testenv.Records(t, brokers, dlqTopic)
		if len(recs) != 1 {
			t.Fatalf("expected 1 DLQ record, got %d", len(recs))
		}

		testenv.Eventually(t, 3*time.Second, func() bool {
			return e.drained(t)
		}, "source record committed after DLQ ack")
	})

	t.Run("commit", func(t *testing.T) {
		if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
			t.Skip("kfake fault injection")
		}
		cluster, ctx := testenv.Kfake(t), testCtx(t)
		brokers := cluster.ListenAddrs()
		e := env{brokers, testenv.Topic(t, brokers, 1), testenv.Group(t), 1}
		e.produce(t, "m", 1)

		commitInFlight := make(chan struct{}, 1)
		unblockCommit := make(chan struct{})
		var (
			intercepted atomic.Bool
			unblockOnce sync.Once
		)
		t.Cleanup(func() {
			unblockOnce.Do(func() { close(unblockCommit) })
		})

		cluster.ControlKey(int16(kmsg.OffsetCommit), func(req kmsg.Request) (kmsg.Response, error, bool) {
			if intercepted.CompareAndSwap(false, true) {
				select {
				case commitInFlight <- struct{}{}:
				default:
				}
				cluster.SleepControl(func() {
					<-unblockCommit
				})
			}
			return nil, nil, false
		})

		cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
			return nil
		})
		r := start(t, ctx, cfg)

		select {
		case <-commitInFlight:
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for commit in flight")
		}
		r.cancel()
		unblockOnce.Do(func() { close(unblockCommit) })

		err := r.wait(t, 3*time.Second)
		if err != nil {
			t.Fatalf("expected nil on cancel during commit, got: %v", err)
		}
		testenv.Eventually(t, 3*time.Second, func() bool {
			return e.drained(t)
		}, "record committed despite cancel during commit")
	})
}

// Case 4: A handler that ignores ctx and blocks after cancel.
// Behavior: Run waits indefinitely for running handlers to finish, and if they
// return nil, commits their records before returning nil.
// Uses a release gate after cancel to guarantee exact return and exit order.
func TestHandlerBlocks3sAfterCancel(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "m", 1)

	started := make(chan struct{})
	release := make(chan struct{})
	var (
		releaseOnce sync.Once
		handlerDone atomic.Bool
		runExited   atomic.Bool
	)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
	})

	cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
		close(started)
		<-release
		handlerDone.Store(true)
		if runExited.Load() {
			t.Error("Run exited before handler finished")
		}
		return nil
	})

	r := start(t, ctx, cfg)
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for handler to start")
	}

	r.cancel()

	// Verify Run stays blocked while handler is waiting on release
	select {
	case <-r.exited:
		t.Fatal("Run exited while handler was still blocked")
	case <-time.After(500 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })

	if err := r.wait(t, 5*time.Second); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	runExited.Store(true)

	if !handlerDone.Load() {
		t.Error("handler never completed")
	}

	testenv.Eventually(t, 5*time.Second, func() bool {
		return e.drained(t)
	}, "record committed after blocking handler returned nil")
}

// Case 5: Panics and Goexit in several partitions at once.
// Asserts CONTRACT.md Guarantee 3 (recovered panics count as errors, Goexit stops Run)
// and Guarantee 4 (commits prefix of other settled partitions).
func TestConcurrentPanicsAndGoexit(t *testing.T) {
	e, ctx := newEnv(t, 4), testCtx(t)
	e.produce(t, "m", 2) // 2 records per partition: offsets 0 and 1

	var (
		p3Settled  = make(chan struct{})
		p0Panicked = make(chan struct{})
		p3Once     sync.Once
		p0Once     sync.Once
	)
	t.Cleanup(func() {
		p3Once.Do(func() { close(p3Settled) })
		p0Once.Do(func() { close(p0Panicked) })
	})

	cfg := e.cfg(func(hctx context.Context, r *kgo.Record) error {
		switch r.Partition {
		case 0:
			// Ensure partition 3 has settled its prefix offset 0 first
			select {
			case <-p3Settled:
			case <-time.After(5 * time.Second):
			}
			panic("p0 panic")
		case 1:
			// Wait until partition 0 has panicked and batch context is cancelled, then Goexit
			select {
			case <-hctx.Done():
			case <-time.After(5 * time.Second):
			}
			runtime.Goexit()
		case 2:
			select {
			case <-hctx.Done():
			case <-time.After(5 * time.Second):
			}
			panic("p2 panic")
		case 3:
			if r.Offset == 0 {
				p3Once.Do(func() { close(p3Settled) })
				return nil // settles offset 0
			}
			select {
			case <-hctx.Done():
			case <-time.After(5 * time.Second):
			}
			runtime.Goexit()
		}
		return nil
	})
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	err := r.wait(t, 10*time.Second)
	if err == nil {
		t.Fatal("expected Run to return an error, got nil")
	}

	errStr := err.Error()
	if !strings.Contains(errStr, "panic") {
		t.Errorf("expected error to mention panic, got: %v", errStr)
	}
	if !strings.Contains(errStr, "runtime.Goexit") {
		t.Errorf("expected error to mention runtime.Goexit, got: %v", errStr)
	}

	testenv.Eventually(t, 5*time.Second, func() bool {
		c := e.committed(t)
		return c[3] == 1 && c[0] == 0 && c[1] == 0 && c[2] == 0
	}, "partition 3 settled offset 0 committed while failed partitions stay at 0")
}

// Case 6: Three members joining and leaving repeatedly (flapping) while records flow.
// Asserts CONTRACT.md Guarantee 1 (at-least-once delivery; no lost records),
// Guarantee 4 (final commits reach end), and Guarantee 5 (rebalance coordination).
// Produces distinct waves across membership changes, gates during rebalance,
// verifies BlockRebalanceOnPoll prevents concurrent partition execution, and
// requires all members (including non-A) to actively handle records without errors.
func TestRebalanceFlappingMembers(t *testing.T) {
	const (
		numParts    = 4
		recsPerPart = 8
	)
	e, ctx := newEnv(t, numParts), testCtx(t)

	var (
		mu        sync.Mutex
		handled   = make(map[string]int)
		handledBy = make(map[string]int)
		wave1ByB  atomic.Int32

		blocking  atomic.Bool
		blocked   = make(chan struct{})
		release   = make(chan struct{})
		closeOnce sync.Once
	)
	t.Cleanup(func() {
		closeOnce.Do(func() { close(release) })
	})

	handler := func(who string) offsetkeeper.Handler {
		return func(ctx context.Context, r *kgo.Record) error {
			key := fmt.Sprintf("%d-%d", r.Partition, r.Offset)
			mu.Lock()
			handled[key]++
			handledBy[who]++
			if who == "B" && strings.HasPrefix(string(r.Value), "w1-") {
				wave1ByB.Add(1)
			}
			mu.Unlock()

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

	startMember := func(name string) *runner {
		cfg := e.cfg(handler(name))
		return start(t, ctx, cfg)
	}

	// Wave 1: initial records for Member A
	e.produce(t, "w1", recsPerPart)
	mA := startMember("A")

	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for member A to block in batch")
	}

	// Member B joins while A is holding its batch open.
	// BlockRebalanceOnPoll ensures B does not handle records until A finishes batch.
	mB := startMember("B")
	testenv.Eventually(t, 10*time.Second, func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "PreparingRebalance" || members == 2
	}, "group rebalance started on member B join")

	// Hold A open while B would wrongly be assigned partitions if BlockRebalanceOnPoll is missing
	window, tick := time.NewTimer(time.Second), time.NewTicker(20*time.Millisecond)
	defer tick.Stop()
	for held := true; held; {
		select {
		case <-window.C:
			held = false
		case <-tick.C:
			mu.Lock()
			bCount := handledBy["B"]
			mu.Unlock()
			if bCount != 0 {
				t.Fatalf("member B handled %d records while A batch was open: rebalance did not wait", bCount)
			}
		}
	}

	closeOnce.Do(func() { close(release) })

	// Wait for wave 1 to settle and the group to stabilize with 2 members
	testenv.Eventually(t, 10*time.Second, func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "Stable" && members == 2 && e.drained(t)
	}, "group stable with members A and B")

	// If BlockRebalanceOnPoll is missing, member B is handed A's uncommitted partitions mid-batch and duplicates wave 1
	if count := wave1ByB.Load(); count != 0 {
		t.Fatalf("member B handled %d records of wave 1: rebalance did not wait for A's batch", count)
	}

	// Wave 2: records produced for both A and B
	e.produce(t, "w2", 2)
	testenv.Eventually(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["B"] > 0
	}, "member B handled wave 2 records")

	// Member C joins, Member A leaves
	mC := startMember("C")
	mA.cancel()
	if err := mA.wait(t, 5*time.Second); err != nil {
		t.Fatalf("member A exit error: %v", err)
	}

	testenv.Eventually(t, 10*time.Second, func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "Stable" && members == 2
	}, "group stable with members B and C")

	// Wave 3: records produced for B and C
	e.produce(t, "w3", 2)
	testenv.Eventually(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["C"] > 0
	}, "member C handled wave 3 records")

	// Member B leaves, Member A rejoins as A2
	mB.cancel()
	if err := mB.wait(t, 5*time.Second); err != nil {
		t.Fatalf("member B exit error: %v", err)
	}

	mA2 := startMember("A2")
	testenv.Eventually(t, 10*time.Second, func() bool {
		state, members := groupState(t, e.brokers, e.group)
		return state == "Stable" && members == 2
	}, "group stable with members C and A2")

	// Wave 4: records produced for C and A2
	e.produce(t, "w4", 2)
	testenv.Eventually(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["A2"] > 0
	}, "member A2 handled wave 4 records")

	// Wait for all partitions to drain
	testenv.Eventually(t, 20*time.Second, func() bool {
		return e.drained(t)
	}, "all partitions drained")

	mC.cancel()
	mA2.cancel()
	if err := mC.wait(t, 5*time.Second); err != nil {
		t.Fatalf("member C exit error: %v", err)
	}
	if err := mA2.wait(t, 5*time.Second); err != nil {
		t.Fatalf("member A2 exit error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, who := range []string{"A", "B", "C", "A2"} {
		if handledBy[who] == 0 {
			t.Errorf("member %s never handled any records", who)
		}
	}
}

// Case 7: Partitions added to topic while Run is running (kadm CreatePartitions).
// Asserts CONTRACT.md Guarantee 2 (per-partition consumption).
// Verifies offsets (2,0) and (3,0) specifically and checks committed next offset is 1.
func TestPartitionsAddedWhileRunning(t *testing.T) {
	e, ctx := newEnv(t, 2), testCtx(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	var c calls
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		c.add(r, "")
		return nil
	})
	cfg.ClientOpts = append(cfg.ClientOpts, kgo.MetadataMinAge(50*time.Millisecond), kgo.MetadataMaxAge(200*time.Millisecond))

	e.produce(t, "p0-1", 2, 0, 1)
	r := start(t, ctx, cfg)
	r.until(t, "initial 4 records consumed", func() bool { return c.total() == 4 })

	// Add 2 partitions (total 4)
	res, err := adm.CreatePartitions(ctx, 2, e.topic)
	if err != nil {
		t.Fatalf("CreatePartitions error: %v", err)
	}
	for _, resp := range res {
		if resp.Err != nil {
			t.Fatalf("CreatePartitions topic error: %v", resp.Err)
		}
	}

	// Produce to new partitions: (2,0) and (3,0)
	testenv.Produce(t, e.brokers,
		&kgo.Record{Topic: e.topic, Partition: 2, Value: []byte("new-p2")},
		&kgo.Record{Topic: e.topic, Partition: 3, Value: []byte("new-p3")},
	)

	// Wait for both new partitions to be consumed at offset 0
	r.until(t, "both new partitions consumed at offset 0", func() bool {
		p2 := c.count(func(s seen) bool { return s.partition == 2 && s.offset == 0 })
		p3 := c.count(func(s seen) bool { return s.partition == 3 && s.offset == 0 })
		return p2 >= 1 && p3 >= 1
	})

	r.cancel()
	if err := r.wait(t, 5*time.Second); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	// Verify committed checkpoints for new partitions
	testenv.Eventually(t, 5*time.Second, func() bool {
		commits := e.committed(t)
		return commits[2] == 1 && commits[3] == 1 && e.drained(t)
	}, "partitions 2 and 3 committed at offset 1 and topic drained")
}

// Case 8: Topic deleted while Run is running.
// Documents actual behavior: Run handles topic deletion gracefully without hang or panic.
// It continues running and polling, and exits cleanly with nil upon context cancellation.
// Characterizes behavior: post-delete fetch is observed, runner remains live, clean exit on cancel.
func TestTopicDeletedWhileRunning(t *testing.T) {
	if os.Getenv("OFFSETKEEPER_BROKERS") != "" {
		t.Skip("kfake fetch observation")
	}
	cluster, ctx := testenv.Kfake(t), testCtx(t)
	brokers := cluster.ListenAddrs()
	e := env{brokers, testenv.Topic(t, brokers, 2), testenv.Group(t), 2}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	var (
		topicDeleted    atomic.Bool
		postDeleteFetch atomic.Bool
	)
	cluster.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if topicDeleted.Load() {
			postDeleteFetch.Store(true)
		}
		return nil, nil, false
	})

	var c calls
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		c.add(r, "")
		return nil
	})
	cfg.ClientOpts = append(cfg.ClientOpts, kgo.MetadataMinAge(50*time.Millisecond), kgo.MetadataMaxAge(200*time.Millisecond))

	e.produce(t, "p", 2, 0, 1)
	r := start(t, ctx, cfg)
	r.until(t, "initial 4 records consumed", func() bool { return c.total() == 4 })

	testenv.Eventually(t, waitFor, func() bool { return e.drained(t) }, "initial records committed")

	// Delete topic
	res, err := adm.DeleteTopics(ctx, e.topic)
	if err != nil {
		t.Fatalf("DeleteTopics error: %v", err)
	}
	for _, resp := range res {
		if resp.Err != nil {
			t.Fatalf("DeleteTopics topic error: %v", resp.Err)
		}
	}
	topicDeleted.Store(true)

	// Verify post-delete fetch is observed while Run remains alive
	testenv.Eventually(t, 10*time.Second, func() bool {
		return postDeleteFetch.Load()
	}, "post-delete fetch observed")

	if !r.running() {
		t.Fatal("Run exited prematurely upon topic deletion")
	}

	r.cancel()
	if err := r.wait(t, 5*time.Second); err != nil {
		t.Fatalf("Run failed to exit cleanly on cancel after topic deletion: %v", err)
	}
}

// Case 9: Fatal error in one partition while another partition's handler is mid-retry backoff.
// Asserts CONTRACT.md Guarantee 3 (fatal error aborts Run), Guarantee 6 (context cancellation
// aborts backoff without waiting full delay; interrupted partition is unsettled), and
// Guarantee 4 (unsettled partitions are not committed).
func TestFatalErrorDuringRetryBackoff(t *testing.T) {
	e, ctx := newEnv(t, 2), testCtx(t)
	e.produce(t, "m", 2) // partitions 0 and 1

	var (
		p1Attempts     atomic.Int32
		p1Attempt1Done = make(chan struct{})
		p1SignalOnce   sync.Once
		p0WaitDone     = make(chan struct{})
		t0             time.Time
		t0Mu           sync.Mutex
	)
	t.Cleanup(func() {
		p1SignalOnce.Do(func() { close(p1Attempt1Done) })
	})

	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		if r.Partition == 1 {
			att := p1Attempts.Add(1)
			if att == 1 {
				// Signal partition 0 after attempt 1 fails and it enters backoff
				defer p1SignalOnce.Do(func() { close(p1Attempt1Done) })
				return errors.New("temporary error")
			}
			return nil
		}
		// Partition 0 waits on bounded select for partition 1 to enter backoff, then fails fatally
		select {
		case <-p1Attempt1Done:
		case <-time.After(10 * time.Second):
			return errors.New("timeout waiting for partition 1 to enter backoff")
		}
		t0Mu.Lock()
		t0 = time.Now()
		t0Mu.Unlock()
		close(p0WaitDone)
		return offsetkeeper.Permanent(errBoom)
	})
	cfg.Backoff = 5 * time.Second
	cfg.MaxAttempts = 3

	r := start(t, ctx, cfg)
	err := r.wait(t, 10*time.Second)

	t0Mu.Lock()
	cancelTime := t0
	t0Mu.Unlock()
	elapsed := time.Since(cancelTime)

	if err == nil || !errors.Is(err, errBoom) {
		t.Fatalf("expected Run to return errBoom, got: %v", err)
	}

	// Backoff was 5s; partition 0's fatal error cancels batch context, aborting partition 1 backoff immediately
	if elapsed >= 3*time.Second {
		t.Errorf("expected backoff to abort immediately on fatal error, but elapsed was %v", elapsed)
	}

	if att := p1Attempts.Load(); att != 1 {
		t.Errorf("partition 1 attempted %d times, expected 1", att)
	}

	testenv.Eventually(t, 5*time.Second, func() bool {
		c := e.committed(t)
		return c[0] == 0 && c[1] == 0
	}, "no offsets committed for unsettled partitions")
}
