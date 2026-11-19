// Package testenv gives tests a Kafka-compatible broker plus small helpers that
// work the same on kfake, Apache Kafka and Redpanda.
package testenv

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seq     atomic.Int64
	unsafe  = regexp.MustCompile(`[^A-Za-z0-9._-]`)
	brokerT = 60 * time.Second
)

// Brokers returns the seed brokers: OFFSETKEEPER_BROKERS (comma separated) once
// it answers metadata, else a fresh in-process kfake cluster closed on cleanup.
func Brokers(t testing.TB) []string {
	t.Helper()
	if env := os.Getenv("OFFSETKEEPER_BROKERS"); env != "" {
		brokers := strings.Split(env, ",")
		cl := client(t, brokers)
		adm := kadm.NewClient(cl)
		var last error
		Eventually(t, brokerT, func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, last = adm.ListBrokers(ctx)
			return last == nil
		}, "broker "+env+" to answer metadata")
		return brokers
	}
	return Kfake(t).ListenAddrs()
}

// Kfake starts an in-process single-broker cluster, closed on cleanup. Tests
// that inject broker faults (Cluster.ControlKey) use it directly; they must
// skip when OFFSETKEEPER_BROKERS is set, because a real broker cannot be told
// to fail.
func Kfake(t testing.TB) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func name(t testing.TB, kind string) string {
	n := unsafe.ReplaceAllString(t.Name(), "_")
	if len(n) > 60 {
		n = n[:60]
	}
	return fmt.Sprintf("ok-%s-%s-%d-%d", kind, n, time.Now().UnixNano(), seq.Add(1))
}

// Group returns a consumer group name unique to this call.
func Group(t testing.TB) string { return name(t, "group") }

// Topic creates a uniquely named topic with the given partition count.
func Topic(t testing.TB, brokers []string, partitions int32) string {
	t.Helper()
	topic := name(t, "topic")
	adm := kadm.NewClient(client(t, brokers))
	Eventually(t, 30*time.Second, func() bool { // a fresh real broker may still be electing a controller
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := adm.CreateTopic(ctx, partitions, -1, nil, topic)
		if err == nil {
			err = resp.Err
		}
		if err != nil {
			t.Logf("create topic %s: %v", topic, err)
		}
		return err == nil
	}, "topic "+topic+" to be created")
	return topic
}

// Produce synchronously produces rs. Each record goes to its own Partition
// field (zero value = partition 0); Topic must be set.
func Produce(t testing.TB, brokers []string, rs ...*kgo.Record) {
	t.Helper()
	cl := client(t, brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, rs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// Committed returns the group's committed offset (next offset to read) per
// partition of topic. Partitions with no commit are absent.
func Committed(t testing.TB, brokers []string, group, topic string) map[int32]int64 {
	t.Helper()
	adm := kadm.NewClient(client(t, brokers))
	var resp kadm.OffsetResponses
	Eventually(t, 30*time.Second, func() bool { // a fresh real broker may still be creating __consumer_offsets
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var err error
		if resp, err = adm.FetchOffsets(ctx, group); err != nil {
			t.Logf("fetch offsets of %s: %v", group, err)
		}
		return err == nil
	}, "offsets of "+group+" to be fetchable")
	out := map[int32]int64{}
	for p, o := range resp[topic] {
		if o.Err != nil {
			t.Fatalf("fetch offset %s/%d: %v", topic, p, o.Err)
		}
		if o.At >= 0 {
			out[p] = o.At
		}
	}
	return out
}

// End returns the end offset (next offset to be written) per partition of topic.
func End(t testing.TB, brokers []string, topic string) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(client(t, brokers)).ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		t.Fatalf("list end offsets of %s: %v", topic, err)
	}
	out := map[int32]int64{}
	for p, o := range ends[topic] {
		out[p] = o.Offset
	}
	return out
}

// Records reads every record of topic from the start up to the end offsets at
// call time, in per-partition order.
func Records(t testing.TB, brokers []string, topic string) []*kgo.Record {
	t.Helper()
	want := 0
	for _, n := range End(t, brokers, topic) {
		want += int(n)
	}
	cl := client(t, brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatalf("read %s: have %d of %d records: %v", topic, len(out), want, err)
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// Eventually polls cond every 10ms until it is true; it fails t after timeout.
// what names the awaited condition in the failure message.
func Eventually(t testing.TB, timeout time.Duration, cond func() bool, what ...string) {
	t.Helper()
	if cond() {
		return
	}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("not met within %s: %s", timeout, strings.Join(what, " "))
		case <-tick.C:
			if cond() {
				return
			}
		}
	}
}

func client(t testing.TB, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("kgo client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}
