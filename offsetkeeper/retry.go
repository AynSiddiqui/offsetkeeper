package offsetkeeper

import (
	"context"
	"errors"
	"time"
)

// maxBackoff caps the doubling so a large MaxAttempts cannot park a partition
// for minutes between attempts.
const maxBackoff = 30 * time.Second

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as not retryable. errors.Is/As still see the wrapped error.
// Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err}
}

// backoffDelay is the wait after the failed-th failed attempt (failed >= 1):
// base, then doubled each time, capped at maxBackoff.
func backoffDelay(base time.Duration, failed int) time.Duration {
	d := base
	// d < maxBackoff before each doubling, so d*2 cannot overflow.
	for i := 1; i < failed && d < maxBackoff; i++ {
		d *= 2
	}
	return min(d, maxBackoff)
}

// retry calls fn up to maxAttempts times and returns how many calls it made and
// the last error (nil if an attempt succeeded). It stops early on a Permanent
// error and when ctx is done: a cancelled run must neither wait out a backoff
// nor start another attempt, so the wait selects on ctx instead of sleeping.
func retry(ctx context.Context, maxAttempts int, base time.Duration, fn func(context.Context) error) (int, error) {
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return attempt, nil
		}
		var perm *permanentError
		if attempt >= maxAttempts || errors.As(err, &perm) || ctx.Err() != nil {
			return attempt, err
		}
		timer := time.NewTimer(backoffDelay(base, attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempt, err
		case <-timer.C:
		}
		// select picks at random when both are ready; a cancelled run must not
		// start another attempt.
		if ctx.Err() != nil {
			return attempt, err
		}
	}
}
