// Package dlq builds, lists and replays dead-letter records.
package dlq

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Header names added to every dead-lettered record. Values are decimal strings
// except the error text.
const (
	HeaderSourceTopic     = "offsetkeeper.source.topic"
	HeaderSourcePartition = "offsetkeeper.source.partition"
	HeaderSourceOffset    = "offsetkeeper.source.offset"
	HeaderError           = "offsetkeeper.error"
	HeaderAttempts        = "offsetkeeper.attempts"

	// maxErrorBytes caps the HeaderError value, marker included. An unbounded
	// handler error (say, one wrapping an HTTP body) could make the DLQ record
	// too large for the broker, and Run would then stop on every restart.
	maxErrorBytes = 4 << 10
	truncMarker   = "…(truncated)"

	// headerPrefix marks every header offsetkeeper owns; replay strips them.
	headerPrefix = "offsetkeeper."
)

// Entry is one DLQ record with its decoded offsetkeeper.* headers. Headers that
// are missing or malformed decode to their zero value.
type Entry struct {
	Record          *kgo.Record
	SourceTopic     string
	SourcePartition int32
	SourceOffset    int64
	Attempts        int
	Err             string
}

// Headers returns src's headers plus the offsetkeeper.* headers. The error text
// is cut to 4 KiB with a "…(truncated)" marker. src is not modified.
func Headers(src *kgo.Record, attempts int, err error) []kgo.RecordHeader {
	msg := ""
	if err != nil {
		msg = err.Error()
	}
	if len(msg) > maxErrorBytes {
		cut := maxErrorBytes - len(truncMarker)
		for cut > 0 && !utf8.RuneStart(msg[cut]) { // never split a multi-byte rune
			cut--
		}
		msg = msg[:cut] + truncMarker
	}
	// Fresh slice: appending to src.Headers could write into its backing array.
	hs := make([]kgo.RecordHeader, 0, len(src.Headers)+5)
	hs = append(hs, src.Headers...)
	return append(hs,
		kgo.RecordHeader{Key: HeaderSourceTopic, Value: []byte(src.Topic)},
		kgo.RecordHeader{Key: HeaderSourcePartition, Value: []byte(strconv.FormatInt(int64(src.Partition), 10))},
		kgo.RecordHeader{Key: HeaderSourceOffset, Value: []byte(strconv.FormatInt(src.Offset, 10))},
		kgo.RecordHeader{Key: HeaderError, Value: []byte(msg)},
		kgo.RecordHeader{Key: HeaderAttempts, Value: []byte(strconv.Itoa(attempts))},
	)
}

// decode reads the offsetkeeper.* headers. The last occurrence of a key wins:
// Headers appends ours after the source's, so a source header that happens to
// use an offsetkeeper.* name cannot shadow the real value.
func decode(r *kgo.Record) Entry {
	e := Entry{Record: r}
	for _, h := range r.Headers {
		v := string(h.Value)
		switch h.Key {
		case HeaderSourceTopic:
			e.SourceTopic = v
		case HeaderSourcePartition:
			// ParseInt and Atoi return the saturated maximum on a range error; a
			// header that does not parse must decode to zero, as Entry documents.
			n, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				n = 0
			}
			e.SourcePartition = int32(n)
		case HeaderSourceOffset:
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				n = 0
			}
			e.SourceOffset = n
		case HeaderError:
			e.Err = v
		case HeaderAttempts:
			n, err := strconv.Atoi(v)
			if err != nil {
				n = 0
			}
			e.Attempts = n
		}
	}
	return e
}

// bounds returns the start and end offsets of every partition of topic.
func bounds(ctx context.Context, cl *kgo.Client, topic string) (start, end map[int32]int64, err error) {
	adm := kadm.NewClient(cl)
	ss, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = ss.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("dlq: start offsets of %s: %w", topic, err)
	}
	es, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = es.Error()
	}
	if err != nil {
		return nil, nil, fmt.Errorf("dlq: end offsets of %s: %w", topic, err)
	}
	start, end = map[int32]int64{}, map[int32]int64{}
	for p, o := range ss[topic] {
		start[p] = o.Offset
	}
	for p, o := range es[topic] {
		end[p] = o.Offset
	}
	if len(end) == 0 {
		return nil, nil, fmt.Errorf("dlq: topic %s has no partitions (does it exist?)", topic)
	}
	return start, end, nil
}

// fetchErr returns the first error that stops read from finishing: a fetch-wide
// error (empty Topic: ctx done, client closed) or an error on a partition read
// wants. Err0 would see only the first partition of the first topic, so a failing
// partition 1 would leave read polling until ctx expires. Errors on other
// topics or partitions are not ours and are ignored.
func fetchErr(fs kgo.Fetches, topic string, want map[int32]kgo.Offset) error {
	for _, fe := range fs.Errors() {
		if fe.Topic == "" {
			return fmt.Errorf("dlq: read %s: %w", topic, fe.Err)
		}
		if _, ok := want[fe.Partition]; ok && fe.Topic == topic {
			return fmt.Errorf("dlq: read %s/%d: %w", topic, fe.Partition, fe.Err)
		}
	}
	return nil
}

// read calls fn with every record of topic in [from[p], to[p]) per partition,
// in offset order within a partition, then returns. It uses cl as a direct
// (group-less) consumer and removes the partitions again before returning, so
// cl can be reused.
//
// shortcut: a partition counts as finished when a record at or past to[p]-1
// arrives. If the tail of the range is a transaction marker or compacted away
// and nothing follows, read waits until ctx ends. DLQ topics are plain
// append-only topics, so upgrade only if that stops being true.
func read(ctx context.Context, cl *kgo.Client, topic string, from, to map[int32]int64, fn func(*kgo.Record) error) error {
	offsets := map[int32]kgo.Offset{}
	var added []int32
	for p, f := range from {
		if f < to[p] {
			offsets[p] = kgo.NewOffset().At(f)
			added = append(added, p)
		}
	}
	if len(offsets) == 0 {
		return nil
	}
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets})
	defer cl.RemoveConsumePartitions(map[string][]int32{topic: added})

	done := map[int32]bool{}
	for len(done) < len(offsets) {
		fs := cl.PollFetches(ctx)
		if fs.IsClientClosed() {
			return kgo.ErrClientClosed
		}
		if err := fetchErr(fs, topic, offsets); err != nil {
			return err
		}
		for _, r := range fs.Records() {
			if _, want := offsets[r.Partition]; !want || done[r.Partition] {
				continue
			}
			if r.Offset < to[r.Partition] {
				if err := fn(r); err != nil {
					return err
				}
			}
			if r.Offset+1 >= to[r.Partition] {
				done[r.Partition] = true
			}
		}
	}
	return nil
}

// List returns every DLQ record from the start to the end offsets at call time,
// ordered by partition then offset.
func List(ctx context.Context, cl *kgo.Client, topic string) ([]Entry, error) {
	start, end, err := bounds(ctx, cl, topic)
	if err != nil {
		return nil, err
	}
	var out []Entry
	err = read(ctx, cl, topic, start, end, func(r *kgo.Record) error {
		out = append(out, decode(r))
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(out, func(a, b Entry) int {
		return cmp.Or(cmp.Compare(a.Record.Partition, b.Record.Partition), cmp.Compare(a.Record.Offset, b.Record.Offset))
	})
	return out, nil
}

// replay re-produces e's record to its source topic with the original key,
// value and headers, and returns only once the broker acked it.
func replay(ctx context.Context, cl *kgo.Client, e Entry) error {
	r := e.Record
	if e.SourceTopic == "" {
		return fmt.Errorf("dlq: %s/%d offset %d has no %s header, cannot tell where to replay it", r.Topic, r.Partition, r.Offset, HeaderSourceTopic)
	}
	out := &kgo.Record{Topic: e.SourceTopic, Key: r.Key, Value: r.Value}
	for _, h := range r.Headers {
		if !strings.HasPrefix(h.Key, headerPrefix) {
			out.Headers = append(out.Headers, h)
		}
	}
	if err := cl.ProduceSync(ctx, out).FirstErr(); err != nil {
		return fmt.Errorf("dlq: replay %s/%d offset %d to %s: %w", r.Topic, r.Partition, r.Offset, e.SourceTopic, err)
	}
	return nil
}

// ReplayOne re-produces one DLQ record to its source topic. It is independent of
// ReplayAll's tracking: it neither reads nor moves the replay group's offset, so
// a later ReplayAll replays that record again.
func ReplayOne(ctx context.Context, cl *kgo.Client, topic string, partition int32, offset int64) error {
	start, end, err := bounds(ctx, cl, topic)
	if err != nil {
		return err
	}
	if s, ok := start[partition]; !ok || offset < s || offset >= end[partition] {
		return fmt.Errorf("dlq: %s/%d has no record at offset %d (available: [%d, %d))", topic, partition, offset, start[partition], end[partition])
	}
	found := false
	err = read(ctx, cl, topic, map[int32]int64{partition: offset}, map[int32]int64{partition: offset + 1}, func(r *kgo.Record) error {
		found = true
		return replay(ctx, cl, decode(r))
	})
	if err == nil && !found {
		err = fmt.Errorf("dlq: %s/%d offset %d no longer exists", topic, partition, offset)
	}
	return err
}

// ReplayAll re-produces every record not yet replayed (tracked by consumer
// group "offsetkeeper-replay."+topic) and returns how many it replayed. The
// group's offset moves only after the produce is acked, so a failure leaves the
// failed record, and everything after it, for the next call.
func ReplayAll(ctx context.Context, cl *kgo.Client, topic string) (int, error) {
	start, end, err := bounds(ctx, cl, topic)
	if err != nil {
		return 0, err
	}
	group := "offsetkeeper-replay." + topic
	adm := kadm.NewClient(cl)
	committed, err := adm.FetchOffsets(ctx, group)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		return 0, fmt.Errorf("dlq: fetch offsets of %s: %w", group, err)
	}
	from := map[int32]int64{}
	for p, s := range start {
		from[p] = s
		// An unset offset has At == -1; retention may also have moved start past
		// the commit, in which case the deleted records are gone and start wins.
		if o, ok := committed[topic][p]; ok {
			from[p] = max(s, o.At)
		}
	}
	n := 0
	err = read(ctx, cl, topic, from, end, func(r *kgo.Record) error {
		if err := replay(ctx, cl, decode(r)); err != nil {
			return err
		}
		n++
		// Per record, not per batch: a crash replays at most the one record
		// that was acked but not yet committed.
		cm := kadm.Offsets{}
		cm.AddOffset(topic, r.Partition, r.Offset+1, -1)
		resp, err := adm.CommitOffsets(ctx, group, cm)
		if err == nil {
			err = resp.Error()
		}
		if err != nil {
			return fmt.Errorf("dlq: commit %s for %s/%d offset %d: %w", group, topic, r.Partition, r.Offset, err)
		}
		return nil
	})
	return n, err
}
