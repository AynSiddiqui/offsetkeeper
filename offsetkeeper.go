package offsetkeeper

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultMaxAttempts = 3
	defaultBackoff     = 100 * time.Millisecond

	// maxPollRecords bounds a batch: while a rebalance is in progress, its
	// handlers and retries must finish within the group rebalance timeout, so a
	// batch must not be unbounded.
	maxPollRecords = 500
	// commitTimeout bounds every batch commit. A commit must survive the cancel
	// that ended the handlers (shutdown, error exit), so it ignores ctx and
	// needs its own limit.
	commitTimeout = 10 * time.Second
)

// Handler processes one record. Its return value is the only settlement:
// nil = done; error = retry, then DLQ (or stop). Wrap with Permanent to skip retries.
//
// r is a copy made for this attempt: the handler may change or re-publish its
// fields (a produce rewrites its Partition and Offset) and the commit and the
// DLQ record still use the record as it was polled. The contents of Key, Value
// and Headers are shared with that record and must not be modified in place; the
// DLQ keeps them. Handler is called concurrently for different partitions and
// sequentially, in offset order, within one partition, so state shared across
// records needs its own locking. A record can be delivered more than once (a
// crash or rebalance before its commit, a replay), so make the handler idempotent.
type Handler func(ctx context.Context, r *kgo.Record) error

// Config configures Run. Brokers, Group, Topics and Handler are required.
type Config struct {
	Brokers []string // required; becomes kgo.SeedBrokers
	Group   string   // required consumer group
	Topics  []string // required
	Handler Handler  // required
	// MaxAttempts is the total attempts per record; 0 means 3; 1 means no retries.
	// Retries run inside the poll batch: a batch is up to 500 records per poll,
	// handled in order per partition, and the worst-case time of all its retries
	// (the sum of the backoffs, per failing record) should stay under the group
	// rebalance timeout (kgo default 60s). The timeout applies only while a
	// rebalance is in progress: if one starts mid-batch and the batch outlives it,
	// the group evicts this member and the batch's records are redelivered
	// elsewhere. Without a rebalance a long batch is harmless; heartbeats continue.
	// Example: MaxAttempts=5, Backoff=10s waits 10+20+30+30 = 90s for one record.
	MaxAttempts int
	// Backoff is the delay before the 2nd attempt, doubled each time, capped at
	// 30s; 0 means 100ms. See MaxAttempts for the budget it must fit in.
	Backoff time.Duration
	// DLQTopic "" means Run returns the error instead of dead-lettering. It must
	// not be in Topics (rejected); if ClientOpts adds kgo.ConsumeRegex, make the
	// pattern exclude it, since that cannot be checked here.
	DLQTopic string
	Logger   *slog.Logger // nil means slog.Default()
	// ClientOpts are extra kgo options (TLS, SASL, tuning). offsetkeeper appends
	// its own group, commit and rebalance opts last, so those cannot be undone.
	// Producer options are not protected: kgo.RequiredAcks(kgo.LeaderAck()) or
	// kgo.NoAck(), and kgo.DisableIdempotentWrite, weaken the DLQ produce and void
	// the guarantee that a dead letter is durable before its source is committed.
	ClientOpts []kgo.Opt
}

// withDefaults validates c and fills defaults. It does not modify c.
func (c Config) withDefaults() (Config, error) {
	switch {
	case len(c.Brokers) == 0:
		return c, errors.New("offsetkeeper: Config.Brokers is required")
	case c.Group == "":
		return c, errors.New("offsetkeeper: Config.Group is required")
	case len(c.Topics) == 0:
		return c, errors.New("offsetkeeper: Config.Topics is required")
	case c.Handler == nil:
		return c, errors.New("offsetkeeper: Config.Handler is required")
	case c.MaxAttempts < 0:
		return c, fmt.Errorf("offsetkeeper: Config.MaxAttempts must be >= 0, got %d", c.MaxAttempts)
	case c.Backoff < 0:
		return c, fmt.Errorf("offsetkeeper: Config.Backoff must be >= 0, got %s", c.Backoff)
	case c.DLQTopic != "" && slices.Contains(c.Topics, c.DLQTopic):
		// A dead letter would be consumed again, fail again and be re-dead-lettered forever.
		return c, fmt.Errorf("offsetkeeper: Config.DLQTopic %q must not be in Config.Topics", c.DLQTopic)
	}
	if i := slices.Index(c.Brokers, ""); i >= 0 {
		return c, fmt.Errorf("offsetkeeper: Config.Brokers[%d] must not be empty", i)
	}
	if i := slices.Index(c.Topics, ""); i >= 0 {
		return c, fmt.Errorf("offsetkeeper: Config.Topics[%d] must not be empty", i)
	}
	if c.MaxAttempts == 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.Backoff == 0 {
		c.Backoff = defaultBackoff
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c, nil
}

// worker is the state shared by the per-partition goroutines of one Run.
type worker struct {
	cfg Config
	cl  *kgo.Client
}

// partResult is one partition's outcome for a batch. Each goroutine writes only
// its own partResult; Run reads them after wg.Wait.
type partResult struct {
	settled *kgo.Record // last record of the contiguous settled prefix; nil if none
	cur     *kgo.Record // record being handled now; names the culprit if the goroutine dies
	err     error       // non-nil: a record could not be settled and Run must stop
}

// Run consumes until ctx is cancelled (returns nil) or a record cannot be settled
// (returns the error). If the final commit fails, that error is returned even
// after a cancel. It creates and closes its own kgo.Client and validates cfg first.
func Run(ctx context.Context, cfg Config) error {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return err
	}
	// Ours go last: later options win, so a ClientOpts value cannot turn
	// auto-commit back on or drop the rebalance gate the commit path relies on.
	opts := append(slices.Clone(cfg.ClientOpts),
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("offsetkeeper: create client: %w", err)
	}
	// Close alone hangs if the last poll did not AllowRebalance (error exit).
	defer cl.CloseAllowingRebalance()

	w := &worker{cfg: cfg, cl: cl}
	// Checking ctx first means a Run that starts cancelled never polls.
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, maxPollRecords)
		// Closed client or done ctx: this is the shutdown path, not a failure.
		// The previous batch was already committed, so nothing is pending.
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			cfg.Logger.Warn("offsetkeeper: fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		if err := w.batch(ctx, fetches); err != nil {
			return err
		}
		// The loop condition stops here after a cancel: no poll, since kgo
		// would re-take the rebalance gate for nothing.
		cl.AllowRebalance()
	}
	return nil
}

// batch handles one poll: one goroutine per partition, then one synchronous
// commit of every partition's settled prefix. It commits even when it is about
// to return an error or the ctx is done, so work already settled is not redone.
func (w *worker) batch(ctx context.Context, fetches kgo.Fetches) error {
	// A failure in one partition cancels the others: Run is about to return, so
	// their remaining records would only be redelivered. Handlers that fail
	// because of this cancel are unsettled, same as on shutdown.
	bctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Group by topic/partition ourselves: EachPartition may visit one partition
	// several times (split fetches), which would run two goroutines on it at once
	// and let a later chunk's commit jump past an earlier chunk's failure. One
	// goroutine per partition then holds by construction.
	type topicPart struct {
		topic     string
		partition int32
	}
	byPart := map[topicPart][]*kgo.Record{}
	fetches.EachRecord(func(r *kgo.Record) {
		k := topicPart{r.Topic, r.Partition}
		byPart[k] = append(byPart[k], r)
	})

	var (
		wg      sync.WaitGroup
		results []*partResult
	)
	for _, recs := range byPart {
		// kgo hands a partition's records out in offset order; sorting makes the
		// order we commit by independent of that.
		slices.SortFunc(recs, func(a, b *kgo.Record) int { return cmp.Compare(a.Offset, b.Offset) })
		res := &partResult{}
		results = append(results, res)
		wg.Go(func() {
			returned := false
			defer func() {
				if returned {
					return
				}
				// The goroutine is unwinding without returning: runtime.Goexit
				// (t.FailNow, t.Skip, a library) that recover() cannot stop.
				// res.settled still holds the prefix settled so far, but the
				// record in hand is neither settled nor failed, and kgo will not
				// redeliver the rest of the batch, so Run must stop or the next
				// batch's commit would skip it.
				res.err = fmt.Errorf("offsetkeeper: handler for %s/%d offset %d did not return (runtime.Goexit)", res.cur.Topic, res.cur.Partition, res.cur.Offset)
				cancel()
			}()
			w.partition(bctx, recs, res)
			returned = true
			if res.err != nil {
				cancel()
			}
		})
	}
	wg.Wait()

	var toCommit []*kgo.Record
	var errs []error
	for _, res := range results {
		if res.settled != nil {
			toCommit = append(toCommit, res.settled)
		}
		if res.err != nil {
			errs = append(errs, res.err)
		}
	}
	if len(toCommit) > 0 {
		// WithoutCancel: the commit must survive the cancel that ended the handlers.
		cctx, ccancel := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
		defer ccancel()
		// CommitRecords commits offset+1 of the highest record per partition with
		// no contiguity check, so we pass only the last record of each settled prefix.
		if err := w.cl.CommitRecords(cctx, toCommit...); err != nil {
			errs = append(errs, fmt.Errorf("offsetkeeper: commit: %w", err))
		}
	}
	return errors.Join(errs...)
}

// partition settles recs (one partition, offset order) one at a time and records
// the last record of the settled prefix in res.settled as each record settles, so
// the prefix survives even if this goroutine dies midway (see batch). It stops at
// the first record that is not settled: a later record must never be handled, or
// committed, past it. res.err stays nil when it stopped because ctx is done.
func (w *worker) partition(ctx context.Context, recs []*kgo.Record, res *partResult) {
	for _, r := range recs {
		if ctx.Err() != nil {
			return
		}
		res.cur = r
		ok, err := w.settle(ctx, r)
		if !ok {
			res.err = err
			return
		}
		// Set even if ctx was cancelled meanwhile: the handler already returned
		// nil, and shutdown must commit such records.
		res.settled = r
	}
}

// settle reports whether r is settled: its handler returned nil, or its DLQ
// record was acked. (false, nil) means ctx ended first and r stays unsettled;
// (false, err) means Run must stop.
func (w *worker) settle(ctx context.Context, r *kgo.Record) (bool, error) {
	attempts, err := retry(ctx, w.cfg.MaxAttempts, w.cfg.Backoff, func(ctx context.Context) error {
		err := w.handle(ctx, r)
		if err != nil && ctx.Err() == nil {
			w.cfg.Logger.Warn("offsetkeeper: handler failed", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
		}
		return err
	})
	if err == nil {
		return true, nil
	}
	if ctx.Err() != nil {
		// A handler error while ctx is cancelled is usually the handler giving up
		// because of the cancel: not evidence the record is bad, so no DLQ.
		return false, nil
	}
	herr := fmt.Errorf("offsetkeeper: %s/%d offset %d failed after %d attempt(s): %w", r.Topic, r.Partition, r.Offset, attempts, err)
	if w.cfg.DLQTopic == "" {
		return false, herr
	}
	if derr := w.deadLetter(ctx, r, attempts, err); derr != nil {
		if ctx.Err() != nil {
			return false, nil // produce was cut short by shutdown; r stays unsettled
		}
		return false, errors.Join(herr, derr)
	}
	// Without this line a MaxAttempts=1 failure is indistinguishable from a dropped record.
	w.cfg.Logger.Warn("offsetkeeper: dead-lettered", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "dlq_topic", w.cfg.DLQTopic, "attempts", attempts)
	return true, nil
}

// handle calls the handler and turns a panic into an error carrying the panic
// value, so one bad record cannot kill the process or its partition.
func (w *worker) handle(ctx context.Context, r *kgo.Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			w.cfg.Logger.Error("offsetkeeper: handler panic", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()
	// A fresh copy per attempt: kgo's produce path rewrites Partition and Offset
	// on a record it is given, and r is what the commit and the DLQ read.
	c := *r
	return w.cfg.Handler(ctx, &c)
}
