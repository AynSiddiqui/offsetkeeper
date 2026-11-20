package dlq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestEdgeDLQFourPartitions tests DLQ topic with 4 partitions:
//  1. List visits all records in exact offset order [0, 1, 2] per partition.
//  2. ReplayAll covers all 4 partitions, reproducing all 12 unique key/value pairs
//     without loss or duplicates, and commits each partition's offset.
//
// Doc ref: docs/CONTRACT.md §7; internal/dlq/dlq.go List and ReplayAll doc comments.
func TestEdgeDLQFourPartitions(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 4)
	srcTopic := testenv.Topic(t, brokers, 4)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	const numPartitions = 4
	const recsPerPartition = 3
	expectedPairs := make(map[string]string)
	var dlqRecords []*kgo.Record

	for p := int32(0); p < numPartitions; p++ {
		for i := range recsPerPartition {
			k := fmt.Sprintf("key-%d-%d", p, i)
			v := fmt.Sprintf("val-%d-%d", p, i)
			expectedPairs[k] = v

			src := &kgo.Record{
				Topic:     srcTopic,
				Partition: p,
				Offset:    int64(i * 10),
			}
			dlqRecords = append(dlqRecords, &kgo.Record{
				Topic:     dlqTopic,
				Partition: p,
				Key:       []byte(k),
				Value:     []byte(v),
				Headers:   Headers(src, i+1, fmt.Errorf("failure %d-%d", p, i)),
			})
		}
	}
	testenv.Produce(t, brokers, dlqRecords...)

	// Subtest 1: List order within partition
	var listed []Entry
	partitionOffsets := make(map[int32][]int64)
	err := List(ctx, cl, dlqTopic, func(e Entry) error {
		listed = append(listed, e)
		partitionOffsets[e.Record.Partition] = append(partitionOffsets[e.Record.Partition], e.Record.Offset)
		return nil
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listed) != numPartitions*recsPerPartition {
		t.Fatalf("List got %d records, want %d", len(listed), numPartitions*recsPerPartition)
	}

	// Verify exact offsets [0, 1, 2] per partition
	for p := int32(0); p < numPartitions; p++ {
		offs := partitionOffsets[p]
		if len(offs) != recsPerPartition {
			t.Fatalf("partition %d has %d listed offsets, want %d", p, len(offs), recsPerPartition)
		}
		for i, o := range offs {
			if o != int64(i) {
				t.Errorf("partition %d offset at index %d = %d, want %d", p, i, o, i)
			}
		}
	}

	// Subtest 2: ReplayAll covers all 4 partitions
	replayedCount, err := ReplayAll(ctx, cl, dlqTopic)
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if replayedCount != numPartitions*recsPerPartition {
		t.Fatalf("ReplayAll returned %d, want %d", replayedCount, numPartitions*recsPerPartition)
	}

	// Verify all 12 unique source key/value pairs arrived without duplication or substitution
	srcRecords := testenv.Records(t, brokers, srcTopic)
	if len(srcRecords) != numPartitions*recsPerPartition {
		t.Fatalf("source topic %s has %d records, want %d", srcTopic, len(srcRecords), numPartitions*recsPerPartition)
	}

	gotPairs := make(map[string]string)
	for _, r := range srcRecords {
		k := string(r.Key)
		v := string(r.Value)
		if _, exists := gotPairs[k]; exists {
			t.Errorf("duplicate key on source topic: %q", k)
		}
		gotPairs[k] = v
	}
	for k, wantV := range expectedPairs {
		gotV, ok := gotPairs[k]
		if !ok {
			t.Errorf("missing expected key on source topic: %q", k)
		} else if gotV != wantV {
			t.Errorf("key %q value = %q, want %q", k, gotV, wantV)
		}
	}

	// Verify replay group committed offset recsPerPartition for every partition
	replayGroup := "offsetkeeper-replay." + dlqTopic
	committed := testenv.Committed(t, brokers, replayGroup, dlqTopic)
	for p := int32(0); p < numPartitions; p++ {
		if committed[p] != recsPerPartition {
			t.Errorf("partition %d committed offset %d, want %d", p, committed[p], recsPerPartition)
		}
	}

	// ReplayAll again should replay 0 records
	againCount, err := ReplayAll(ctx, cl, dlqTopic)
	if err != nil || againCount != 0 {
		t.Fatalf("second ReplayAll = %d, %v; want 0, nil", againCount, err)
	}
}

// TestEdgeDLQEmptyTopic tests ReplayAll on an empty DLQ topic.
// It returns (0, nil) immediately without error or hang.
// Doc ref: docs/CONTRACT.md §7; internal/dlq/dlq.go ReplayAll doc comment.
func TestEdgeDLQEmptyTopic(t *testing.T) {
	brokers := testenv.Brokers(t)
	emptyTopic := testenv.Topic(t, brokers, 4)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	n, err := ReplayAll(ctx, cl, emptyTopic)
	if err != nil {
		t.Fatalf("ReplayAll on empty topic returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("ReplayAll on empty topic returned %d, want 0", n)
	}
}

// TestEdgeDLQNonexistentTopic tests List, ReplayOne, and ReplayAll on a nonexistent topic.
// Each subtest gets a fresh context, verifies that it returned a missing-topic error,
// and ensures the context was still live at return (not passing on deadline expiration).
// Doc ref: internal/dlq/dlq.go bounds; docs/CONTRACT.md is silent on nonexistent DLQ topic.
func TestEdgeDLQNonexistentTopic(t *testing.T) {
	brokers := testenv.Brokers(t)
	cl := newClient(t, brokers)
	missingTopic := fmt.Sprintf("missing-dlq-%d", time.Now().UnixNano())

	t.Run("List", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		err := List(ctx, cl, missingTopic, func(Entry) error { return nil })
		if ctx.Err() != nil {
			t.Fatalf("List passed only because context expired: %v", ctx.Err())
		}
		if err == nil {
			t.Fatal("List on nonexistent topic returned nil error")
		}
		if !strings.Contains(err.Error(), "has no partitions") && !strings.Contains(err.Error(), "start offsets") && !strings.Contains(err.Error(), "UNKNOWN_TOPIC_OR_PARTITION") {
			t.Errorf("List returned unexpected error: %v", err)
		}
	})

	t.Run("ReplayOne", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		err := ReplayOne(ctx, cl, missingTopic, 0, 0)
		if ctx.Err() != nil {
			t.Fatalf("ReplayOne passed only because context expired: %v", ctx.Err())
		}
		if err == nil {
			t.Fatal("ReplayOne on nonexistent topic returned nil error")
		}
		if !strings.Contains(err.Error(), "has no partitions") && !strings.Contains(err.Error(), "start offsets") && !strings.Contains(err.Error(), "UNKNOWN_TOPIC_OR_PARTITION") {
			t.Errorf("ReplayOne returned unexpected error: %v", err)
		}
	})

	t.Run("ReplayAll", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		n, err := ReplayAll(ctx, cl, missingTopic)
		if ctx.Err() != nil {
			t.Fatalf("ReplayAll passed only because context expired: %v", ctx.Err())
		}
		if err == nil {
			t.Fatal("ReplayAll on nonexistent topic returned nil error")
		}
		if n != 0 {
			t.Errorf("ReplayAll returned %d, want 0", n)
		}
	})
}

// TestEdgeReplayOneInvalidOffsets tests ReplayOne on a deleted offset:
// When DeleteRecords deletes records 0..2 on partition 0 (leaving [3, 5)), ReplayOne(0, 1)
// must return a range error naming partition 0, offset 1, and available range [3, 5),
// with no record produced to the destination topic.
// Doc ref: docs/CONTRACT.md §7 ("expired records cannot be replayed"); internal/dlq/dlq.go ReplayOne doc.
func TestEdgeReplayOneInvalidOffsets(t *testing.T) {
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	// Produce 5 records on partition 0 (offsets 0..4)
	for i := range 5 {
		src := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: int64(i)}
		testenv.Produce(t, brokers, &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       fmt.Appendf(nil, "k-%d", i),
			Value:     fmt.Appendf(nil, "v-%d", i),
			Headers:   Headers(src, 1, errors.New("err")),
		})
	}

	// Delete records 0..2 (offsets < 3) via DeleteRecords
	adm := kadm.NewClient(cl)
	dResp, err := adm.DeleteRecords(ctx, kadm.Offsets{
		topic: {0: kadm.Offset{At: 3}},
	})
	if err == nil {
		err = dResp.Error()
	}
	if err != nil {
		t.Fatalf("DeleteRecords failed: %v", err)
	}

	// Subtest: ReplayOne on deleted offset 1
	subCtx, subCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer subCancel()

	err = ReplayOne(subCtx, cl, topic, 0, 1)
	if subCtx.Err() != nil {
		t.Fatalf("ReplayOne passed only because context expired: %v", subCtx.Err())
	}
	if err == nil {
		t.Fatal("ReplayOne on deleted offset 1 returned nil error")
	}

	wantErrMsg := fmt.Sprintf("%s/0 has no record at offset 1 (available: [3, 5))", topic)
	if !strings.Contains(err.Error(), wantErrMsg) {
		t.Errorf("ReplayOne err = %q, want error containing %q", err.Error(), wantErrMsg)
	}

	// Verify no destination records were produced to srcTopic
	srcRecs := testenv.Records(t, brokers, srcTopic)
	if len(srcRecs) != 0 {
		t.Fatalf("destination topic received %d records, want 0", len(srcRecs))
	}
}

// TestEdgeDLQRecordsWithoutHeadersAndMalformed tests DLQ records without offsetkeeper headers
// and with malformed header values.
// ReplayAll must fail at record 1 with an error naming HeaderSourceTopic and the offending record (topic/0 offset 1).
// Doc ref: internal/dlq/dlq.go Entry and ReplayAll doc comments; docs/CONTRACT.md §7.
func TestEdgeDLQRecordsWithoutHeadersAndMalformed(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	// 1. Record 0: Valid DLQ record
	src0 := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: 10}
	r0 := &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Key:       []byte("k0"),
		Value:     []byte("v0"),
		Headers:   Headers(src0, 1, errors.New("first-err")),
	}

	// 2. Record 1: DLQ record WITHOUT offsetkeeper headers (hand-produced)
	r1 := &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Key:       []byte("k1-no-headers"),
		Value:     []byte("v1"),
		Headers: []kgo.RecordHeader{
			{Key: "custom-header", Value: []byte("val")},
		},
	}

	// 3. Record 2: DLQ record with malformed header values (corrupt/non-numeric)
	r2 := &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Key:       []byte("k2-malformed"),
		Value:     []byte("v2"),
		Headers: []kgo.RecordHeader{
			{Key: HeaderSourceTopic, Value: []byte(srcTopic)},
			{Key: HeaderSourcePartition, Value: []byte("not-a-number")},
			{Key: HeaderSourceOffset, Value: []byte("invalid-offset-value")},
			{Key: HeaderAttempts, Value: []byte("corrupted-attempts")},
			{Key: HeaderError, Value: []byte("error-value-ok")},
		},
	}

	// 4. Record 3: Valid DLQ record
	src3 := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: 30}
	r3 := &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Key:       []byte("k3"),
		Value:     []byte("v3"),
		Headers:   Headers(src3, 2, errors.New("third-err")),
	}

	testenv.Produce(t, brokers, r0, r1, r2, r3)

	// List verification: List must show all records sanely without panicking
	var entries []Entry
	err := List(ctx, cl, dlqTopic, func(e Entry) error {
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		t.Fatalf("List failed on DLQ with missing/malformed headers: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("List returned %d entries, want 4", len(entries))
	}

	// Entry 1 (no offsetkeeper headers): must decode to zero values
	if entries[1].SourceTopic != "" || entries[1].SourcePartition != 0 || entries[1].SourceOffset != 0 || entries[1].Attempts != 0 || entries[1].Err != "" {
		t.Errorf("entry 1 decoded with non-zero values: %+v", entries[1])
	}

	// Entry 2 (malformed non-numeric headers): must decode numeric fields to zero values, preserve topic and err
	if entries[2].SourceTopic != srcTopic {
		t.Errorf("entry 2 source topic = %q, want %q", entries[2].SourceTopic, srcTopic)
	}
	if entries[2].SourcePartition != 0 || entries[2].SourceOffset != 0 || entries[2].Attempts != 0 {
		t.Errorf("entry 2 malformed numbers did not decode to 0: partition=%d, offset=%d, attempts=%d",
			entries[2].SourcePartition, entries[2].SourceOffset, entries[2].Attempts)
	}
	if entries[2].Err != "error-value-ok" {
		t.Errorf("entry 2 error = %q, want 'error-value-ok'", entries[2].Err)
	}

	// ReplayAll verification:
	// Must fail at record 1 with error naming HeaderSourceTopic and offending record dlqTopic/0 offset 1
	n, err := ReplayAll(ctx, cl, dlqTopic)
	if err == nil {
		t.Fatalf("ReplayAll succeeded, want error due to missing %s", HeaderSourceTopic)
	}
	if n != 1 {
		t.Fatalf("ReplayAll returned count %d, want 1 (count replayed before the failure)", n)
	}

	wantSubstr := fmt.Sprintf("%s/0 offset 1 has no %s header", dlqTopic, HeaderSourceTopic)
	if !strings.Contains(err.Error(), wantSubstr) {
		t.Errorf("ReplayAll error = %q, want substring %q", err.Error(), wantSubstr)
	}

	// Checkpoint verification: replay group must be committed at offset 1 (record 1 not committed)
	replayGroup := "offsetkeeper-replay." + dlqTopic
	committed := testenv.Committed(t, brokers, replayGroup, dlqTopic)
	if committed[0] != 1 {
		t.Errorf("replay group committed offset %d, want 1", committed[0])
	}

	// Source topic verification: only record 0 should be on the source topic
	srcRecs := testenv.Records(t, brokers, srcTopic)
	if len(srcRecs) != 1 || string(srcRecs[0].Key) != "k0" {
		t.Fatalf("source topic has %v, want exactly [k0]", srcRecs)
	}
}

// TestEdgeReplaySourceTopicDeleted tests ReplayOne when destination topic was deleted.
// It must fail with missing-destination cause with a fresh context.
// Doc ref: docs/CONTRACT.md §7; internal/dlq/dlq.go ReplayOne doc comment.
func TestEdgeReplaySourceTopicDeleted(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	// Produce 1 record to DLQ targeting srcTopic
	srcRecord := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: 100}
	testenv.Produce(t, brokers, &kgo.Record{
		Topic:   dlqTopic,
		Key:     []byte("k0"),
		Value:   []byte("v0"),
		Headers: Headers(srcRecord, 1, errors.New("err0")),
	})

	// Delete srcTopic
	adm := kadm.NewClient(cl)
	dResp, err := adm.DeleteTopics(ctx, srcTopic)
	if err == nil {
		err = dResp.Error()
	}
	if err != nil {
		t.Fatalf("DeleteTopics failed: %v", err)
	}

	// Fast-failing client so produce to missing topic fails without retrying indefinitely
	fastCl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.UnknownTopicRetries(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fastCl.Close)

	subCtx, subCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer subCancel()

	err = ReplayOne(subCtx, fastCl, dlqTopic, 0, 0)
	if subCtx.Err() != nil {
		t.Fatalf("ReplayOne passed only because context expired: %v", subCtx.Err())
	}
	if err == nil {
		t.Fatal("ReplayOne on deleted source topic returned nil error")
	}
	if !strings.Contains(err.Error(), "replay") || !strings.Contains(err.Error(), srcTopic) {
		t.Errorf("ReplayOne error = %q, want cause naming destination %s", err.Error(), srcTopic)
	}
}

// cancelOnProduceHook cancels context when triggerAt produce promises finish.
type cancelOnProduceHook struct {
	targetTopic string
	triggerAt   int32
	count       atomic.Int32
	cancel      context.CancelFunc
}

func (h *cancelOnProduceHook) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if r.Topic == h.targetTopic && err == nil {
		if h.count.Add(1) == h.triggerAt {
			h.cancel()
		}
	}
}

// TestEdgeReplayAllContextCancelResumed tests ReplayAll interrupted mid-way by context cancellation.
// Requires hook count 2, errors.Is(err1, context.Canceled), n1=2, and first-run durable keys/checkpoint
// before resuming. When resumed: no records lost, <= 1 duplicate.
// Doc ref: docs/CONTRACT.md §7 ("at most the one record that was acked but not yet committed").
func TestEdgeReplayAllContextCancelResumed(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)

	const totalRecords = 5
	var dlqRecs []*kgo.Record
	for i := range totalRecords {
		src := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: int64(i * 10)}
		dlqRecs = append(dlqRecs, &kgo.Record{
			Topic:     dlqTopic,
			Partition: 0,
			Key:       fmt.Appendf(nil, "key-%d", i),
			Value:     fmt.Appendf(nil, "val-%d", i),
			Headers:   Headers(src, 1, fmt.Errorf("err-%d", i)),
		})
	}
	testenv.Produce(t, brokers, dlqRecs...)

	intCtx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	hook := &cancelOnProduceHook{
		targetTopic: srcTopic,
		triggerAt:   2,
		cancel:      cancel,
	}

	intCl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.WithHooks(hook),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intCl.Close)

	// First run - interrupted by cancel
	n1, err1 := ReplayAll(intCtx, intCl, dlqTopic)
	if err1 == nil {
		t.Fatalf("first ReplayAll returned nil error, want context cancel error; n=%d", n1)
	}
	if !errors.Is(err1, context.Canceled) && !strings.Contains(err1.Error(), "context canceled") {
		t.Fatalf("err1 = %v, want errors.Is(err1, context.Canceled)", err1)
	}
	if hook.count.Load() != 2 {
		t.Fatalf("hook count = %d, want exactly 2 produces", hook.count.Load())
	}
	if n1 != 2 {
		t.Fatalf("first run replayed count n1 = %d, want 2", n1)
	}

	// Verify first-run durable checkpoint and keys before resuming
	replayGroup := "offsetkeeper-replay." + dlqTopic
	committedBefore := testenv.Committed(t, brokers, replayGroup, dlqTopic)
	if committedBefore[0] < 1 || committedBefore[0] > 2 {
		t.Fatalf("pre-resume checkpoint = %d, want 1 or 2", committedBefore[0])
	}
	srcRecsBefore := testenv.Records(t, brokers, srcTopic)
	if len(srcRecsBefore) != 2 {
		t.Fatalf("pre-resume source topic records = %d, want 2", len(srcRecsBefore))
	}

	// Resume ReplayAll with fresh context and client
	resumeCtx, resumeCancel := testCtx(t)
	defer resumeCancel()
	cleanCl := newClient(t, brokers)

	n2, err2 := ReplayAll(resumeCtx, cleanCl, dlqTopic)
	if err2 != nil {
		t.Fatalf("resumed ReplayAll failed: %v", err2)
	}

	// Verify all records reached source topic
	srcRecs := testenv.Records(t, brokers, srcTopic)
	seenKeys := map[string]int{}
	for _, r := range srcRecs {
		seenKeys[string(r.Key)]++
	}

	// 1. No record lost
	for i := range totalRecords {
		k := fmt.Sprintf("key-%d", i)
		if seenKeys[k] == 0 {
			t.Errorf("missing key %q on source topic after resume", k)
		}
	}

	// 2. At most one duplicate record
	if len(srcRecs) < totalRecords || len(srcRecs) > totalRecords+1 {
		t.Errorf("source topic record count = %d, want between %d and %d", len(srcRecs), totalRecords, totalRecords+1)
	}

	// 3. n1 + n2 == len(srcRecs)
	if n1+n2 != len(srcRecs) {
		t.Errorf("sum of replayed counts (n1=%d + n2=%d = %d) != actual source records (%d)",
			n1, n2, n1+n2, len(srcRecs))
	}

	// Final ReplayAll should produce 0 records
	n3, err3 := ReplayAll(resumeCtx, cleanCl, dlqTopic)
	if err3 != nil || n3 != 0 {
		t.Fatalf("third ReplayAll = %d, %v; want 0, nil", n3, err3)
	}
}

// TestEdgeDLQTerminalTransactionMarker tests List and ReplayAll hitting a terminal transaction marker.
// Safe contract across kfake, Kafka, and Redpanda:
// 1. List and ReplayAll return within a bound (nil if broker exposes progress, or a ctx error if broker never does).
// 2. Every visible record is listed and replayed exactly once.
// 3. The checkpoint never points before an unreplayed visible record nor past the log end.
// Doc ref: docs/CONTRACT.md §7 ("If a DLQ topic contains an invisible terminal marker... list or replay waits for a visible record or cancellation (Ctrl-C)").
func TestEdgeDLQTerminalTransactionMarker(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)

	// Produce 1 visible record via transaction and commit it.
	// This results in offset 0 (visible record) and offset 1 (terminal commit control marker).
	txCl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(fmt.Sprintf("tx-dlq-edge-%d", time.Now().UnixNano())),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(txCl.Close)

	ctx, cancel := testCtx(t)
	defer cancel()

	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	src := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: 10}
	if err := txCl.ProduceSync(ctx, &kgo.Record{
		Topic:     dlqTopic,
		Partition: 0,
		Key:       []byte("visible-key"),
		Value:     []byte("visible-val"),
		Headers:   Headers(src, 1, errors.New("visible-err")),
	}).FirstErr(); err != nil {
		t.Fatalf("ProduceSync in transaction failed: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction(TryCommit) failed: %v", err)
	}

	cl := newClient(t, brokers)
	endOffsets := testenv.End(t, brokers, dlqTopic)
	dlqEnd := endOffsets[0]

	// Subtest 1: List
	t.Run("List", func(t *testing.T) {
		listCtx, listCancel := context.WithTimeout(t.Context(), 1*time.Second)
		defer listCancel()

		var listed []Entry
		start := time.Now()
		err := List(listCtx, cl, dlqTopic, func(e Entry) error {
			listed = append(listed, e)
			return nil
		})
		elapsed := time.Since(start)

		if elapsed > 3*time.Second {
			t.Errorf("List took %v, want bounded return within 3s", elapsed)
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context") {
			t.Errorf("List returned unexpected non-context error: %v", err)
		}
		// Every visible record is listed exactly once
		if len(listed) != 1 || string(listed[0].Record.Key) != "visible-key" {
			t.Errorf("List saw %d records, want exactly 1 visible record: %v", len(listed), listed)
		}
	})

	// Subtest 2: ReplayAll
	t.Run("ReplayAll", func(t *testing.T) {
		repCtx, repCancel := context.WithTimeout(t.Context(), 1*time.Second)
		defer repCancel()

		start := time.Now()
		n, err := ReplayAll(repCtx, cl, dlqTopic)
		elapsed := time.Since(start)

		if elapsed > 3*time.Second {
			t.Errorf("ReplayAll took %v, want bounded return within 3s", elapsed)
		}
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "context") {
			t.Errorf("ReplayAll returned unexpected non-context error: %v", err)
		}
		if n != 1 {
			t.Errorf("ReplayAll count = %d, want 1 (visible record)", n)
		}

		// Source topic must have received the visible record exactly once
		srcRecs := testenv.Records(t, brokers, srcTopic)
		if len(srcRecs) != 1 || string(srcRecs[0].Key) != "visible-key" {
			t.Errorf("source topic has %v, want exactly 1 record with key 'visible-key'", srcRecs)
		}

		// Checkpoint must never point before an unreplayed visible record (>= 1)
		// nor past the log end (<= dlqEnd)
		replayGroup := "offsetkeeper-replay." + dlqTopic
		committed := testenv.Committed(t, brokers, replayGroup, dlqTopic)
		cm := committed[0]
		if cm < 1 || cm > dlqEnd {
			t.Errorf("replay group committed offset %d, want between 1 (settled visible) and %d (log end)", cm, dlqEnd)
		}
	})
}

// TestEdgeReplayRetentionPastCheckpoint tests replay after retention moves the low watermark
// past an existing checkpoint (via DeleteRecords):
// Only retained keys are reproduced, and the checkpoint advances to the end.
// Doc ref: docs/CONTRACT.md §7 ("retention may also have moved start past the commit, in which case the deleted records are gone and start wins").
func TestEdgeReplayRetentionPastCheckpoint(t *testing.T) {
	brokers := testenv.Brokers(t)
	dlqTopic := testenv.Topic(t, brokers, 1)
	srcTopic := testenv.Topic(t, brokers, 1)
	cl := newClient(t, brokers)

	ctx, cancel := testCtx(t)
	defer cancel()

	// Produce 6 records to dlqTopic targeting srcTopic (offsets 0..5)
	const totalRecords = 6
	for i := range totalRecords {
		src := &kgo.Record{Topic: srcTopic, Partition: 0, Offset: int64(i * 10)}
		testenv.Produce(t, brokers, &kgo.Record{
			Topic:     dlqTopic,
			Partition: 0,
			Key:       fmt.Appendf(nil, "key-%d", i),
			Value:     fmt.Appendf(nil, "val-%d", i),
			Headers:   Headers(src, 1, fmt.Errorf("err-%d", i)),
		})
	}

	// Set initial checkpoint at offset 2 (records 0 and 1 previously processed)
	replayGroup := "offsetkeeper-replay." + dlqTopic
	adm := kadm.NewClient(cl)
	cm := kadm.Offsets{}
	cm.AddOffset(dlqTopic, 0, 2, -1)
	if resp, err := adm.CommitOffsets(ctx, replayGroup, cm); err != nil || resp.Error() != nil {
		t.Fatalf("initial CommitOffsets failed: %v / %v", err, resp.Error())
	}

	// Now retention moves low watermark past the checkpoint (delete up to offset 4)
	dResp, err := adm.DeleteRecords(ctx, kadm.Offsets{
		dlqTopic: {0: kadm.Offset{At: 4}},
	})
	if err == nil {
		err = dResp.Error()
	}
	if err != nil {
		t.Fatalf("DeleteRecords failed: %v", err)
	}

	// Run ReplayAll: start offset 4 wins over checkpoint 2
	n, err := ReplayAll(ctx, cl, dlqTopic)
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	// Only offsets 4 and 5 are retained and should be replayed (count = 2)
	if n != 2 {
		t.Fatalf("ReplayAll replayed %d records, want 2 (only retained records)", n)
	}

	// Verify only retained keys arrived on srcTopic
	srcRecs := testenv.Records(t, brokers, srcTopic)
	if len(srcRecs) != 2 {
		t.Fatalf("source topic has %d records, want 2", len(srcRecs))
	}
	gotKeys := []string{string(srcRecs[0].Key), string(srcRecs[1].Key)}
	if gotKeys[0] != "key-4" || gotKeys[1] != "key-5" {
		t.Errorf("replayed keys = %v, want [key-4, key-5]", gotKeys)
	}

	// Verify checkpoint advanced to the end offset (6)
	committedAfter := testenv.Committed(t, brokers, replayGroup, dlqTopic)
	if committedAfter[0] != 6 {
		t.Errorf("committed offset after ReplayAll = %d, want 6 (end of partition)", committedAfter[0])
	}

	// Subsequent ReplayAll should replay 0 records
	nAgain, err := ReplayAll(ctx, cl, dlqTopic)
	if err != nil || nAgain != 0 {
		t.Fatalf("subsequent ReplayAll = %d, %v; want 0, nil", nAgain, err)
	}
}

// FuzzHeadersDecodeRoundTrip fuzzes Headers and decode to verify round-trip integrity:
// Headers then decode returns the same source fields for arbitrary inputs.
// Exact retained prefix plus marker is verified for errors exceeding maxErrorBytes.
// Doc ref: internal/dlq/dlq.go Headers and decode; docs/CONTRACT.md §7.
func FuzzHeadersDecodeRoundTrip(f *testing.F) {
	seeds := []struct {
		topic     string
		partition int32
		offset    int64
		attempts  int
		errMsg    string
	}{
		{"orders", 0, 0, 1, "error message"},
		{"t", 3, 42, 5, "kaboom"},
		{"special.chars-topic_1", -1, -1, 0, ""},
		{"unicode-🚀", 100, 9999999999, 10, "Unicode: 错误 ⚠️"},
		{"boundary-ascii", 1, 10, 2, strings.Repeat("a", maxErrorBytes-len(truncMarker))},
		{"boundary-ascii-plus1", 1, 10, 2, strings.Repeat("a", maxErrorBytes-len(truncMarker)+1)},
		{"boundary-multibyte-é", 1, 10, 2, strings.Repeat("é", (maxErrorBytes-len(truncMarker))/2+5)},
		{"boundary-multibyte-🚀", 1, 10, 2, strings.Repeat("🚀", (maxErrorBytes-len(truncMarker))/4+5)},
		{"long-err", 1, 10, 2, strings.Repeat("long error text ", 300)},
	}
	for _, tc := range seeds {
		f.Add(tc.topic, tc.partition, tc.offset, tc.attempts, tc.errMsg)
	}

	f.Fuzz(func(t *testing.T, topic string, partition int32, offset int64, attempts int, errMsg string) {
		src := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		}

		var inputErr error
		if errMsg != "" {
			inputErr = errors.New(errMsg)
		}

		hs := Headers(src, attempts, inputErr)
		e := decode(&kgo.Record{Headers: hs})

		if e.SourceTopic != topic {
			t.Errorf("SourceTopic = %q, want %q", e.SourceTopic, topic)
		}
		if e.SourcePartition != partition {
			t.Errorf("SourcePartition = %d, want %d", e.SourcePartition, partition)
		}
		if e.SourceOffset != offset {
			t.Errorf("SourceOffset = %d, want %d", e.SourceOffset, offset)
		}
		if e.Attempts != attempts {
			t.Errorf("Attempts = %d, want %d", e.Attempts, attempts)
		}

		// Error verification
		if len(errMsg) <= maxErrorBytes {
			if e.Err != errMsg {
				t.Errorf("Err = %q, want %q", e.Err, errMsg)
			}
		} else {
			// Capped at 4 KiB with truncMarker
			if len(e.Err) > maxErrorBytes {
				t.Errorf("Err length %d exceeds maxErrorBytes %d", len(e.Err), maxErrorBytes)
			}
			if !strings.HasSuffix(e.Err, truncMarker) {
				t.Errorf("Err does not end with truncMarker: %q", e.Err)
			}

			// Verify exact retained prefix matching production cut algorithm
			cut := maxErrorBytes - len(truncMarker)
			for cut > 0 && !utf8.RuneStart(errMsg[cut]) {
				cut--
			}
			wantTruncated := errMsg[:cut] + truncMarker
			if e.Err != wantTruncated {
				t.Errorf("Err = %q, want exact truncated prefix %q", e.Err, wantTruncated)
			}
		}
	})
}
