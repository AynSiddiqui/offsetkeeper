package dlq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

func TestHeaders(t *testing.T) {
	src := &kgo.Record{Topic: "orders", Partition: 3, Offset: 42, Headers: []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}}}
	got := Headers(src, 4, errors.New("kaboom"))

	want := map[string]string{
		"h1": "v1", HeaderSourceTopic: "orders", HeaderSourcePartition: "3",
		HeaderSourceOffset: "42", HeaderError: "kaboom", HeaderAttempts: "4",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d headers, want %d: %v", len(got), len(want), got)
	}
	for _, h := range got {
		if want[h.Key] != string(h.Value) {
			t.Errorf("header %s = %q, want %q", h.Key, h.Value, want[h.Key])
		}
	}
	if len(src.Headers) != 1 {
		t.Errorf("src headers were modified: %v", src.Headers)
	}
	if hs := Headers(src, 1, nil); string(hs[len(hs)-2].Value) != "" {
		t.Errorf("nil error must give an empty %s, got %q", HeaderError, hs[len(hs)-2].Value)
	}
	// "é" is 2 bytes, so an odd cut point would split a rune.
	for _, unit := range []string{"x", "é"} {
		msg := strings.Repeat(unit, 1<<20)
		hs := Headers(&kgo.Record{Topic: "t"}, 1, errors.New(msg))
		got := string(hs[len(hs)-2].Value)
		if len(got) > maxErrorBytes || !strings.HasSuffix(got, truncMarker) || !strings.HasPrefix(got, unit) || !utf8.ValidString(got) {
			t.Errorf("1 MiB of %q: header is %d bytes, suffix ok %v, valid UTF-8 %v; want <= %d bytes, marker, valid UTF-8",
				unit, len(got), strings.HasSuffix(got, truncMarker), utf8.ValidString(got), maxErrorBytes)
		}
	}
	if hs := Headers(&kgo.Record{}, 1, errors.New("short")); string(hs[len(hs)-2].Value) != "short" {
		t.Errorf("short error was altered: %q", hs[len(hs)-2].Value)
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	src := &kgo.Record{Topic: "orders", Partition: 3, Offset: 42}
	e := decode(&kgo.Record{Headers: Headers(src, 2, errors.New("kaboom"))})
	if e.SourceTopic != "orders" || e.SourcePartition != 3 || e.SourceOffset != 42 || e.Attempts != 2 || e.Err != "kaboom" {
		t.Errorf("decode = %+v", e)
	}
	// A source header using our name must not shadow the real one (ours come last).
	src.Headers = []kgo.RecordHeader{{Key: HeaderSourceTopic, Value: []byte("liar")}}
	if e := decode(&kgo.Record{Headers: Headers(src, 1, nil)}); e.SourceTopic != "orders" {
		t.Errorf("source topic = %q, want orders", e.SourceTopic)
	}
	// Out-of-range or non-numeric values decode to zero, not to a saturated maximum.
	bad := &kgo.Record{Headers: []kgo.RecordHeader{
		{Key: HeaderSourcePartition, Value: []byte("99999999999")},
		{Key: HeaderSourceOffset, Value: []byte("99999999999999999999")},
		{Key: HeaderAttempts, Value: []byte("99999999999999999999")},
	}}
	if e := decode(bad); e.SourcePartition != 0 || e.SourceOffset != 0 || e.Attempts != 0 {
		t.Errorf("decode of out-of-range headers = partition %d, offset %d, attempts %d, want all 0", e.SourcePartition, e.SourceOffset, e.Attempts)
	}
}

func newClient(t *testing.T, brokers []string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func TestList(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 2)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()
	if got, err := List(ctx, cl, topic); err != nil || len(got) != 0 {
		t.Fatalf("empty topic: %v, %v", got, err)
	}

	var rs []*kgo.Record
	for p := int32(0); p < 2; p++ {
		for i := range 3 {
			src := &kgo.Record{Topic: "src", Partition: p + 5, Offset: int64(i)}
			rs = append(rs, &kgo.Record{
				Topic: topic, Partition: p, Key: []byte("k"), Value: fmt.Appendf(nil, "v-%d-%d", p, i),
				Headers: Headers(src, i+1, fmt.Errorf("err %d", i)),
			})
		}
	}
	testenv.Produce(t, brokers, rs...)

	got, err := List(ctx, cl, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("List returned %d entries, want 6", len(got))
	}
	for i, e := range got {
		p, n := int32(i/3), i%3
		if e.Record.Partition != p || e.Record.Offset != int64(n) {
			t.Errorf("entry %d is %d/%d, want sorted by partition then offset", i, e.Record.Partition, e.Record.Offset)
		}
		if e.SourceTopic != "src" || e.SourcePartition != p+5 || e.SourceOffset != int64(n) || e.Attempts != n+1 || e.Err != fmt.Sprintf("err %d", n) {
			t.Errorf("entry %d decoded as %+v", i, e)
		}
	}

	// read removes its partitions again, so the client can be reused.
	again, err := List(ctx, cl, topic)
	if err != nil || !slices.EqualFunc(got, again, func(a, b Entry) bool {
		return a.Record.Offset == b.Record.Offset && a.Record.Partition == b.Record.Partition
	}) {
		t.Errorf("second List = %d entries, %v", len(again), err)
	}

	if _, err := List(ctx, cl, topic+".missing"); err == nil {
		t.Error("List of a missing topic returned nil")
	}
}

func TestReplayOneRejectsBadOffsets(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	cl := newClient(t, brokers)
	testenv.Produce(t, brokers, &kgo.Record{Topic: topic, Value: []byte("no offsetkeeper headers")})

	ctx, cancel := testCtx(t)
	defer cancel()
	for name, c := range map[string]struct {
		p int32
		o int64
	}{"past end": {0, 1}, "negative": {0, -1}, "no partition": {7, 0}, "no source header": {0, 0}} {
		if err := ReplayOne(ctx, cl, topic, c.p, c.o); err == nil {
			t.Errorf("%s: ReplayOne returned nil", name)
		}
	}
}

func testCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), 30*time.Second)
}

// Err0 sees only partition 0 of the first topic; an error on a wanted partition
// further in must still stop read, while errors that are not ours must not.
func TestFetchErr(t *testing.T) {
	boom := errors.New("boom")
	fs := func(es ...kgo.FetchError) kgo.Fetches {
		var f kgo.Fetch
		byTopic := map[string][]kgo.FetchPartition{}
		for _, e := range es {
			byTopic[e.Topic] = append(byTopic[e.Topic], kgo.FetchPartition{Partition: e.Partition, Err: e.Err})
		}
		for topic, ps := range byTopic {
			f.Topics = append(f.Topics, kgo.FetchTopic{Topic: topic, Partitions: ps})
		}
		return kgo.Fetches{f}
	}
	want := map[int32]kgo.Offset{0: {}, 1: {}}

	if err := fetchErr(fs(kgo.FetchError{Topic: "t", Partition: 0}, kgo.FetchError{Topic: "t", Partition: 1, Err: boom}), "t", want); !errors.Is(err, boom) {
		t.Errorf("error on partition 1 (Err0 is nil here): got %v", err)
	}
	if err := fetchErr(kgo.NewErrFetch(boom), "t", want); !errors.Is(err, boom) {
		t.Errorf("fetch-wide error: got %v", err)
	}
	if err := fetchErr(fs(kgo.FetchError{Topic: "t", Partition: 5, Err: boom}, kgo.FetchError{Topic: "other", Partition: 0, Err: boom}), "t", want); err != nil {
		t.Errorf("errors on unwanted partitions/topics must be ignored: got %v", err)
	}
}

// ReplayAll must not move its group's offset past a record whose produce failed.
func TestReplayAllFailedProduceDoesNotCommit(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	src := &kgo.Record{Topic: topic + ".gone", Partition: 0, Offset: 0} // never created
	testenv.Produce(t, brokers, &kgo.Record{Topic: topic, Value: []byte("x"), Headers: Headers(src, 3, errors.New("kaboom"))})
	// Fail fast on the missing topic instead of retrying until ctx ends.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.UnknownTopicRetries(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	ctx, cancel := testCtx(t)
	defer cancel()
	n, err := ReplayAll(ctx, cl, topic)
	if err == nil || n != 0 {
		t.Fatalf("ReplayAll = %d, %v; want 0 and an error", n, err)
	}
	if got := testenv.Committed(t, brokers, "offsetkeeper-replay."+topic, topic); len(got) != 0 {
		t.Errorf("replay group committed %v though the produce failed", got)
	}
}
