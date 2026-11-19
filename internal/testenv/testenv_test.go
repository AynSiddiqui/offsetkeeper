package testenv

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHelpersRoundTrip(t *testing.T) {
	brokers := Brokers(t)
	topic := Topic(t, brokers, 3)
	g := Group(t) // two calls must differ; a variable keeps staticcheck from flagging the comparison
	if other := Topic(t, brokers, 1); other == topic || g == Group(t) {
		t.Fatal("names are not unique")
	}

	var rs []*kgo.Record
	for p := int32(0); p < 3; p++ {
		for i := 0; i <= int(p); i++ { // partition p gets p+1 records
			rs = append(rs, &kgo.Record{Topic: topic, Partition: p, Value: fmt.Appendf(nil, "p%d-%d", p, i)})
		}
	}
	Produce(t, brokers, rs...)

	want := map[int32]int64{0: 1, 1: 2, 2: 3}
	for p, n := range want {
		if got := End(t, brokers, topic)[p]; got != n {
			t.Fatalf("end[%d] = %d, want %d", p, got, n)
		}
	}
	if got := Records(t, brokers, topic); len(got) != 6 {
		t.Fatalf("Records returned %d records, want 6", len(got))
	}

	group := Group(t)
	if got := Committed(t, brokers, group, topic); len(got) != 0 {
		t.Fatalf("nothing committed yet, got %v", got)
	}
	cm := kadm.Offsets{}
	for p, n := range want {
		cm.AddOffset(topic, p, n, -1)
	}
	ctx := t.Context()
	resp, err := kadm.NewClient(client(t, brokers)).CommitOffsets(ctx, group, cm)
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	got := Committed(t, brokers, group, topic)
	for p, n := range want {
		if got[p] != n {
			t.Fatalf("committed = %v, want %v", got, want)
		}
	}
}

func TestEventually(t *testing.T) {
	n := 0
	Eventually(t, time.Second, func() bool { n++; return n >= 3 })

	f := &fakeTB{TB: t}
	done := make(chan struct{})
	go func() { defer close(done); Eventually(f, 50*time.Millisecond, func() bool { return false }, "never") }()
	<-done
	if f.msg == "" {
		t.Fatal("Eventually did not fail on timeout")
	}
}

// fakeTB records Fatalf instead of failing the real test.
type fakeTB struct {
	testing.TB
	msg string
}

func (f *fakeTB) Helper() {}
func (f *fakeTB) Fatalf(format string, a ...any) {
	f.msg = fmt.Sprintf(format, a...)
	runtime.Goexit()
}
