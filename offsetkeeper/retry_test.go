package offsetkeeper

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBackoffDelay(t *testing.T) {
	base := 100 * time.Millisecond
	for failed, want := range map[int]time.Duration{
		1:   100 * time.Millisecond,
		2:   200 * time.Millisecond,
		3:   400 * time.Millisecond,
		9:   25600 * time.Millisecond, // 100ms * 2^8, still under the cap
		10:  maxBackoff,               // 100ms * 2^9 = 51.2s, capped
		100: maxBackoff,               // no overflow for a huge MaxAttempts
	} {
		if got := backoffDelay(base, failed); got != want {
			t.Errorf("backoffDelay(%s, %d) = %s, want %s", base, failed, got, want)
		}
	}
	if got := backoffDelay(time.Minute, 1); got != maxBackoff {
		t.Errorf("a base above the cap must be capped, got %s", got)
	}
}

func TestRetryAttempts(t *testing.T) {
	boom := errors.New("boom")
	count := 0
	fail := func(context.Context) error { count++; return boom }

	if n, err := retry(t.Context(), 3, time.Nanosecond, fail); n != 3 || count != 3 || !errors.Is(err, boom) {
		t.Errorf("exhausted: attempts=%d calls=%d err=%v, want 3 calls and boom", n, count, err)
	}

	count = 0
	if n, err := retry(t.Context(), 3, time.Nanosecond, func(context.Context) error { count++; return Permanent(boom) }); n != 1 || count != 1 || !errors.Is(err, boom) {
		t.Errorf("permanent: attempts=%d calls=%d err=%v, want one attempt and boom", n, count, err)
	}

	count = 0
	if n, err := retry(t.Context(), 1, time.Nanosecond, fail); n != 1 || count != 1 || err == nil {
		t.Errorf("MaxAttempts=1: attempts=%d calls=%d err=%v, want one attempt", n, count, err)
	}

	count = 0
	n, err := retry(t.Context(), 5, time.Nanosecond, func(context.Context) error {
		if count++; count < 3 {
			return boom
		}
		return nil
	})
	if n != 3 || err != nil {
		t.Errorf("recovering: attempts=%d err=%v, want 3 and nil", n, err)
	}
}

// A cancelled ctx ends a long backoff at once, with no further attempt.
func TestRetryStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	count := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = retry(ctx, 5, time.Hour, func(context.Context) error { count++; return errors.New("boom") })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("retry kept waiting after ctx was cancelled")
	}
	if count != 1 {
		t.Errorf("fn ran %d times, want 1", count)
	}
}
