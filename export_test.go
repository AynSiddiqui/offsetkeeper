package offsetkeeper

import "time"

// SetDLQTimeout shortens dlqTimeout for contract tests and returns the restore
// func. Tests that call it must not run in parallel.
func SetDLQTimeout(d time.Duration) (restore func()) {
	old := dlqTimeout
	dlqTimeout = d
	return func() { dlqTimeout = old }
}
