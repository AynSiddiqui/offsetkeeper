package offsetkeeper_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/AynSiddiqui/offsetkeeper"
	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
)

func testClient(t *testing.T, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("kgo client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// Case 1: nil key, nil value, empty value, tombstones through normal handling, DLQ and replay.
// CONTRACT.md: Guarantee 1 (at-least-once), Guarantee 3 (settlement), Guarantee 7 (DLQ record has same key/value).
// Review verdict: fix (route both empty shapes through DLQ and replay; compare nilness and bytes at each stage).
func TestEdgeRecordShapes_NilEmptyTombstone(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	records := []*kgo.Record{
		{Topic: e.topic, Partition: 0, Key: nil, Value: []byte("val-nil-key")},
		{Topic: e.topic, Partition: 0, Key: []byte("key-tombstone"), Value: nil}, // tombstone
		{Topic: e.topic, Partition: 0, Key: nil, Value: nil},                     // nil key & nil val
		{Topic: e.topic, Partition: 0, Key: []byte("key-empty-val"), Value: []byte{}},
		{Topic: e.topic, Partition: 0, Key: []byte{}, Value: []byte{}}, // empty key & empty val
	}
	testenv.Produce(t, e.brokers, records...)

	var mu sync.Mutex
	var handled []*kgo.Record
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		handled = append(handled, &kgo.Record{
			Key:   slices.Clone(r.Key),
			Value: slices.Clone(r.Value),
		})
		mu.Unlock()
		// Fail records 1, 2, 3, 4 to route tombstone and both empty shapes through DLQ and replay
		if r.Offset >= 1 {
			return errBoom
		}
		return nil
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	r.until(t, "handled all 5 records and dead-lettered 4", func() bool {
		mu.Lock()
		n := len(handled)
		mu.Unlock()
		return n >= 5 && e.total(t, dlqTopic) == 4 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	// Stage 1: Compare nilness and bytes at handler
	if handled[0].Key != nil || string(handled[0].Value) != "val-nil-key" {
		t.Errorf("handler rec 0: got key=%v, val=%v", handled[0].Key, handled[0].Value)
	}
	if string(handled[1].Key) != "key-tombstone" || handled[1].Value != nil {
		t.Errorf("handler rec 1 (tombstone): got key=%v, val=%v", handled[1].Key, handled[1].Value)
	}
	if handled[2].Key != nil || handled[2].Value != nil {
		t.Errorf("handler rec 2 (nil/nil): got key=%v, val=%v", handled[2].Key, handled[2].Value)
	}
	if string(handled[3].Key) != "key-empty-val" || handled[3].Value == nil || len(handled[3].Value) != 0 {
		t.Errorf("handler rec 3 (empty val): got key=%v, val=%v", handled[3].Key, handled[3].Value)
	}
	if handled[4].Key == nil || len(handled[4].Key) != 0 || handled[4].Value == nil || len(handled[4].Value) != 0 {
		t.Errorf("handler rec 4 (empty key/val): got key=%v, val=%v", handled[4].Key, handled[4].Value)
	}

	// Replay all 4 dead-lettered records
	dlqCl := testClient(t, e.brokers)
	replayed, err := dlq.ReplayAll(ctx, dlqCl, dlqTopic)
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if replayed != 4 {
		t.Fatalf("replayed %d records, want 4", replayed)
	}

	// Stage 2: Compare nilness and bytes after replay on source topic
	sourceRecords := testenv.Records(t, e.brokers, e.topic)
	if len(sourceRecords) != 9 { // 5 original + 4 replayed
		t.Fatalf("total source records = %d, want 9", len(sourceRecords))
	}

	// Replayed tombstone (offset 1 replayed -> index 5)
	repTombstone := sourceRecords[5]
	if string(repTombstone.Key) != "key-tombstone" || repTombstone.Value != nil {
		t.Errorf("replayed tombstone: got key=%v val=%v, want key='key-tombstone' val=nil", repTombstone.Key, repTombstone.Value)
	}

	// Replayed nil/nil (offset 2 replayed -> index 6)
	repNilNil := sourceRecords[6]
	if repNilNil.Key != nil || repNilNil.Value != nil {
		t.Errorf("replayed nil/nil: got key=%v val=%v, want nil/nil", repNilNil.Key, repNilNil.Value)
	}

	// Replayed empty val (offset 3 replayed -> index 7)
	repEmptyVal := sourceRecords[7]
	if string(repEmptyVal.Key) != "key-empty-val" || repEmptyVal.Value == nil || len(repEmptyVal.Value) != 0 {
		t.Errorf("replayed empty val: got key=%v, val=%v, want non-nil len=0", repEmptyVal.Key, repEmptyVal.Value)
	}

	// Replayed empty key & empty val (offset 4 replayed -> index 8)
	repEmptyKeyVal := sourceRecords[8]
	if repEmptyKeyVal.Key == nil || len(repEmptyKeyVal.Key) != 0 || repEmptyKeyVal.Value == nil || len(repEmptyKeyVal.Value) != 0 {
		t.Errorf("replayed empty key/val: got key=%v, val=%v, want non-nil len=0 for both", repEmptyKeyVal.Key, repEmptyKeyVal.Value)
	}
}

// Case 2: 1 MiB value through handler, commit, DLQ and replay.
// CONTRACT.md: Guarantee 1, 3, 7, 8. Note: ClientOpts must configure ProducerBatchMaxBytes for >1MB.
// Review verdict: keep (retain sized clients and compressible fixture; claim logical payload size).
func TestEdgeRecordShapes_1MiBValue(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	val := make([]byte, 1<<20) // 1 MiB logical payload
	for i := range val {
		val[i] = byte(i % 251)
	}

	prodCl := testClient(t, e.brokers, kgo.ProducerBatchMaxBytes(2<<20))
	rec := &kgo.Record{Topic: e.topic, Partition: 0, Value: val}
	if err := prodCl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatalf("produce 1MiB: %v", err)
	}

	var mu sync.Mutex
	var handledVal []byte
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		handledVal = bytes.Clone(r.Value)
		mu.Unlock()
		return errBoom // trigger DLQ to verify 1 MiB dead-letter produce
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1
	cfg.ClientOpts = []kgo.Opt{kgo.ProducerBatchMaxBytes(2 << 20)}

	r := start(t, ctx, cfg)
	r.until(t, "handled 1MiB record and dead-lettered", func() bool {
		mu.Lock()
		n := len(handledVal)
		mu.Unlock()
		return n == 1<<20 && e.total(t, dlqTopic) == 1 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	eq := bytes.Equal(handledVal, val)
	mu.Unlock()
	if !eq {
		t.Fatal("handler received corrupted 1MiB value")
	}

	// Verify DLQ and Replay with 1 MiB payload
	replayCl := testClient(t, e.brokers, kgo.ProducerBatchMaxBytes(2<<20))
	replayed, err := dlq.ReplayAll(ctx, replayCl, dlqTopic)
	if err != nil {
		t.Fatalf("ReplayAll: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("replayed %d, want 1", replayed)
	}

	srcRecs := testenv.Records(t, e.brokers, e.topic)
	if len(srcRecs) != 2 { // 1 original + 1 replayed
		t.Fatalf("source topic records = %d, want 2", len(srcRecs))
	}
	if !bytes.Equal(srcRecs[1].Value, val) {
		t.Fatal("replayed 1MiB value corrupted")
	}
}

// Case 3: 500 headers on a record through handler, DLQ, and replay.
// CONTRACT.md: Guarantee 7 ("DLQ record. Same key, value and headers as the source, plus offsetkeeper.*").
// Review verdict: keep (keep full sequence comparison).
func TestEdgeRecordShapes_500Headers(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	var origHeaders []kgo.RecordHeader
	for i := 0; i < 500; i++ {
		origHeaders = append(origHeaders, kgo.RecordHeader{
			Key:   fmt.Sprintf("hdr-%03d", i),
			Value: []byte(fmt.Sprintf("val-%03d", i)),
		})
	}
	rec := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("val"), Headers: origHeaders}
	testenv.Produce(t, e.brokers, rec)

	var mu sync.Mutex
	var gotHeaders []kgo.RecordHeader
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		gotHeaders = slices.Clone(r.Headers)
		mu.Unlock()
		return errBoom // trigger DLQ
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	r.until(t, "handler processed 500 headers and dead-lettered", func() bool {
		mu.Lock()
		n := len(gotHeaders)
		mu.Unlock()
		return n == 500 && e.total(t, dlqTopic) == 1 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	copiedHeaders := slices.Clone(gotHeaders)
	mu.Unlock()

	// Verify all 500 headers reached handler in order
	for i, h := range copiedHeaders {
		wantKey := fmt.Sprintf("hdr-%03d", i)
		wantVal := fmt.Sprintf("val-%03d", i)
		if h.Key != wantKey || string(h.Value) != wantVal {
			t.Fatalf("header %d: got (%s, %s), want (%s, %s)", i, h.Key, h.Value, wantKey, wantVal)
		}
	}

	// Replay and verify all 500 headers preserved and offsetkeeper headers stripped
	replayCl := testClient(t, e.brokers)
	if n, err := dlq.ReplayAll(ctx, replayCl, dlqTopic); err != nil || n != 1 {
		t.Fatalf("ReplayAll: n=%d err=%v", n, err)
	}

	srcRecs := testenv.Records(t, e.brokers, e.topic)
	if len(srcRecs) != 2 {
		t.Fatalf("source records = %d, want 2", len(srcRecs))
	}
	replayedRec := srcRecs[1]
	if len(replayedRec.Headers) != 500 {
		t.Fatalf("replayed headers len = %d, want 500", len(replayedRec.Headers))
	}
	for i, h := range replayedRec.Headers {
		wantKey := fmt.Sprintf("hdr-%03d", i)
		wantVal := fmt.Sprintf("val-%03d", i)
		if h.Key != wantKey || string(h.Value) != wantVal {
			t.Fatalf("replayed header %d: got (%s, %s), want (%s, %s)", i, h.Key, h.Value, wantKey, wantVal)
		}
	}
}

// Case 4: User headers starting with reserved prefix `offsetkeeper.`
// CONTRACT.md: Guarantee 7 ("all headers prefixed with offsetkeeper. are reserved and stripped before replaying").
// Review verdict: keep (keep collision and custom-prefix cases).
func TestEdgeRecordShapes_ReservedHeaders(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	rec := &kgo.Record{
		Topic: e.topic, Partition: 0, Value: []byte("val"),
		Headers: []kgo.RecordHeader{
			{Key: "offsetkeeper.source.topic", Value: []byte("spoofed-topic")},
			{Key: "offsetkeeper.custom.flag", Value: []byte("custom-flag")},
			{Key: "user.app.header", Value: []byte("app-value")},
		},
	}
	testenv.Produce(t, e.brokers, rec)

	var mu sync.Mutex
	var handlerHeaders []kgo.RecordHeader
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		handlerHeaders = slices.Clone(r.Headers)
		mu.Unlock()
		return errBoom
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	r.until(t, "handled and dead-lettered", func() bool {
		mu.Lock()
		n := len(handlerHeaders)
		mu.Unlock()
		return n == 3 && e.total(t, dlqTopic) == 1 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Verify DLQ decode uses real source topic, not spoofed user header
	dlqCl := testClient(t, e.brokers)
	var listEntry dlq.Entry
	err := dlq.List(ctx, dlqCl, dlqTopic, func(e dlq.Entry) error {
		listEntry = e
		return nil
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if listEntry.SourceTopic != e.topic {
		t.Fatalf("DLQ decoded SourceTopic = %q, want real topic %q", listEntry.SourceTopic, e.topic)
	}

	// Replay and verify all offsetkeeper.* headers stripped, app header kept
	if n, err := dlq.ReplayAll(ctx, dlqCl, dlqTopic); err != nil || n != 1 {
		t.Fatalf("ReplayAll: n=%d err=%v", n, err)
	}

	srcRecs := testenv.Records(t, e.brokers, e.topic)
	if len(srcRecs) != 2 {
		t.Fatalf("source records = %d, want 2", len(srcRecs))
	}
	replayed := srcRecs[1]
	for _, h := range replayed.Headers {
		if strings.HasPrefix(h.Key, "offsetkeeper.") {
			t.Errorf("replayed record still has reserved header: %s=%s", h.Key, h.Value)
		}
	}
	if val, ok := header(replayed, "user.app.header"); !ok || val != "app-value" {
		t.Errorf("user.app.header missing or wrong on replayed record: ok=%v val=%s", ok, val)
	}
}

// Case 5: Duplicate header keys.
// CONTRACT.md: Guarantee 7 ("DLQ record. Same key, value and headers as the source...").
// Review verdict: fix (compare full ordered pairs at handler and replay, and check r.wait).
func TestEdgeRecordShapes_DuplicateHeaderKeys(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	headers := []kgo.RecordHeader{
		{Key: "x-dup", Value: []byte("first")},
		{Key: "x-other", Value: []byte("mid")},
		{Key: "x-dup", Value: []byte("second")},
		{Key: "x-dup", Value: []byte("third")},
	}
	rec := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("val"), Headers: headers}
	testenv.Produce(t, e.brokers, rec)

	var mu sync.Mutex
	var handlerHeaders []kgo.RecordHeader
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		handlerHeaders = slices.Clone(r.Headers)
		mu.Unlock()
		return errBoom
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	r.until(t, "handled and dead-lettered", func() bool {
		mu.Lock()
		n := len(handlerHeaders)
		mu.Unlock()
		return n == 4 && e.total(t, dlqTopic) == 1 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	hCopy := slices.Clone(handlerHeaders)
	mu.Unlock()

	// Verify full ordered pairs at handler
	if len(hCopy) != len(headers) {
		t.Fatalf("handler headers len = %d, want %d", len(hCopy), len(headers))
	}
	for i, h := range hCopy {
		if h.Key != headers[i].Key || !bytes.Equal(h.Value, headers[i].Value) {
			t.Fatalf("handler header %d mismatch: got (%s,%s), want (%s,%s)", i, h.Key, h.Value, headers[i].Key, headers[i].Value)
		}
	}

	// Replay and verify duplicate headers and interleaved x-other stay preserved in exact order
	replayCl := testClient(t, e.brokers)
	if n, err := dlq.ReplayAll(ctx, replayCl, dlqTopic); err != nil || n != 1 {
		t.Fatalf("ReplayAll: n=%d err=%v", n, err)
	}

	srcRecs := testenv.Records(t, e.brokers, e.topic)
	if len(srcRecs) != 2 {
		t.Fatalf("source records = %d, want 2", len(srcRecs))
	}
	replayed := srcRecs[1]
	if len(replayed.Headers) != len(headers) {
		t.Fatalf("replayed headers len = %d, want %d", len(replayed.Headers), len(headers))
	}
	for i, h := range replayed.Headers {
		if h.Key != headers[i].Key || !bytes.Equal(h.Value, headers[i].Value) {
			t.Fatalf("replayed header %d mismatch: got (%s,%s), want (%s,%s)", i, h.Key, h.Value, headers[i].Key, headers[i].Value)
		}
	}
}

// Case 6: Non-UTF-8 bytes in key, value, and header key/value through DLQ and replay.
// CONTRACT.md: Guarantee 7 ("DLQ record. Same key, value and headers as the source...").
// Review verdict: fix (require r.wait to return nil).
func TestEdgeRecordShapes_NonUTF8Bytes(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	dlqTopic := testenv.Topic(t, e.brokers, 1)

	rawKey := []byte{0xff, 0xfe, 0x00, 0x80}
	rawVal := []byte{0x80, 0x81, 0xff, 0x00, 0xfe}
	rawHdrKey := string([]byte{0xfe, 0xed, 0x80})
	rawHdrVal := []byte{0xde, 0xad, 0xbe, 0xef, 0xff}

	rec := &kgo.Record{
		Topic: e.topic, Partition: 0,
		Key: rawKey, Value: rawVal,
		Headers: []kgo.RecordHeader{{Key: rawHdrKey, Value: rawHdrVal}},
	}
	testenv.Produce(t, e.brokers, rec)

	var mu sync.Mutex
	var gotKey, gotVal, gotHdrVal []byte
	var gotHdrKey string
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		mu.Lock()
		gotKey = slices.Clone(r.Key)
		gotVal = slices.Clone(r.Value)
		if len(r.Headers) > 0 {
			gotHdrKey = r.Headers[0].Key
			gotHdrVal = slices.Clone(r.Headers[0].Value)
		}
		mu.Unlock()
		return errBoom
	})
	cfg.DLQTopic = dlqTopic
	cfg.MaxAttempts = 1

	r := start(t, ctx, cfg)
	r.until(t, "handled and dead-lettered", func() bool {
		mu.Lock()
		n := len(gotKey)
		mu.Unlock()
		return n > 0 && e.total(t, dlqTopic) == 1 && e.drained(t)
	})
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	mu.Lock()
	kMatch := bytes.Equal(gotKey, rawKey)
	vMatch := bytes.Equal(gotVal, rawVal)
	hkMatch := gotHdrKey == rawHdrKey
	hvMatch := bytes.Equal(gotHdrVal, rawHdrVal)
	mu.Unlock()

	if !kMatch || !vMatch || !hkMatch || !hvMatch {
		t.Fatalf("handler mismatch on non-UTF-8 bytes")
	}

	// Replay and verify byte fidelity
	replayCl := testClient(t, e.brokers)
	if n, err := dlq.ReplayAll(ctx, replayCl, dlqTopic); err != nil || n != 1 {
		t.Fatalf("ReplayAll: n=%d err=%v", n, err)
	}

	srcRecs := testenv.Records(t, e.brokers, e.topic)
	if len(srcRecs) != 2 {
		t.Fatalf("source records = %d, want 2", len(srcRecs))
	}
	replayed := srcRecs[1]
	if !bytes.Equal(replayed.Key, rawKey) {
		t.Errorf("replayed key mismatch: got %x, want %x", replayed.Key, rawKey)
	}
	if !bytes.Equal(replayed.Value, rawVal) {
		t.Errorf("replayed val mismatch: got %x, want %x", replayed.Value, rawVal)
	}
	if len(replayed.Headers) != 1 || replayed.Headers[0].Key != rawHdrKey || !bytes.Equal(replayed.Headers[0].Value, rawHdrVal) {
		t.Errorf("replayed header mismatch")
	}
}

// Case 7a: Transactional source topic with READ_COMMITTED.
// Control records and aborted transactions create offset gaps.
// CONTRACT.md: Guarantee 3 (settlement), Guarantee 4 (commit never past unsettled).
// Review verdict: fix (derive offsets dynamically from produce results/ack to support all brokers including Redpanda).
func TestEdgeOffsetGaps_TransactionalSourceTopic_ReadCommitted(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	txCl := testClient(t, e.brokers,
		kgo.TransactionalID("tx-rc-"+testenv.Group(t)),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Txn 1: Commit record 0 ("tx-0")
	rec0 := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-0")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 1: %v", err)
	}
	if err := txCl.ProduceSync(ctx, rec0).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 1: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 1: %v", err)
	}
	off0 := rec0.Offset

	// Txn 2: Abort record ("tx-aborted") -> creates offset gap
	recAbort := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-aborted")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recAbort).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 2: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction 2: %v", err)
	}

	// Txn 3: Commit record ("tx-interior") -> interior failing record
	recInterior := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-interior")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 3: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recInterior).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 3: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 3: %v", err)
	}
	offInterior := recInterior.Offset

	// Txn 4: Commit record ("tx-successor") -> visible successor in same partition/batch
	recSuccessor := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-successor")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 4: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recSuccessor).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 4: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 4: %v", err)
	}
	offSuccessor := recSuccessor.Offset

	// Verify that aborted transaction created an offset gap
	if offInterior <= off0+1 {
		t.Fatalf("expected gap between off0=%d and offInterior=%d", off0, offInterior)
	}
	if offSuccessor <= offInterior {
		t.Fatalf("expected offSuccessor > offInterior: got %d <= %d", offSuccessor, offInterior)
	}

	// Subcase 1: Interior failure after gap blocks commit and handler never sees successor
	t.Run("FailAfterGapBlocksCommit", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var seenOffsets []int64
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			MaxAttempts: 1,
			ClientOpts:  []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())},
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				seenOffsets = append(seenOffsets, r.Offset)
				mu.Unlock()
				if r.Offset == offInterior {
					return errBoom
				}
				return nil
			},
		}
		err := offsetkeeper.Run(sctx, cfg)
		if err == nil {
			t.Fatal("expected error from Run on interior failing record")
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run error = %v, want errors.Is errBoom", err)
		}

		mu.Lock()
		gotSeen := slices.Clone(seenOffsets)
		mu.Unlock()

		// Handler must have seen [off0, offInterior] only; offSuccessor must NEVER have entered handler
		wantSeen := []int64{off0, offInterior}
		if !slices.Equal(gotSeen, wantSeen) {
			t.Fatalf("handler seen offsets = %v, want exactly %v (successor %d must not run)", gotSeen, wantSeen, offSuccessor)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		if comm[0] > offInterior {
			t.Fatalf("committed %d past failed offset %d", comm[0], offInterior)
		}
		wantComm := off0 + 1
		if comm[0] != wantComm {
			t.Fatalf("committed = %d, want %d (settled prefix offset %d + 1)", comm[0], wantComm, off0)
		}

		// Fresh consumer resuming from committed offset receives [offInterior, offSuccessor] in order
		freshCl := testClient(t, e.brokers,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(e.topic),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		)
		var resumedOffs []int64
		testenv.Eventually(t, waitFor, func() bool {
			fs := freshCl.PollFetches(sctx)
			if err := fs.Err0(); err != nil {
				t.Fatalf("fetch error: %v", err)
			}
			fs.EachRecord(func(r *kgo.Record) {
				resumedOffs = append(resumedOffs, r.Offset)
			})
			return len(resumedOffs) >= 2
		}, fmt.Sprintf("fresh consumer to read [%d, %d]", offInterior, offSuccessor))

		wantResumed := []int64{offInterior, offSuccessor}
		if !slices.Equal(resumedOffs, wantResumed) {
			t.Fatalf("fresh consumer resumed offsets = %v, want %v", resumedOffs, wantResumed)
		}
	})

	// Subcase 2: Fully settled batch asserts exact ordered visible offsets [off0, offInterior, offSuccessor]
	t.Run("FullySettledResumesClean", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var handled []int64
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			ClientOpts: []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())},
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				handled = append(handled, r.Offset)
				mu.Unlock()
				return nil
			},
		}
		rctx, rcancel := context.WithCancel(sctx)
		r := start(t, rctx, cfg)
		r.until(t, "handled all 3 visible records", func() bool {
			mu.Lock()
			n := len(handled)
			mu.Unlock()
			return n >= 3
		})
		rcancel()
		if err := r.wait(t, waitFor); err != nil {
			t.Fatalf("Run: %v", err)
		}

		mu.Lock()
		gotHandled := slices.Clone(handled)
		mu.Unlock()

		wantHandled := []int64{off0, offInterior, offSuccessor}
		if !slices.Equal(gotHandled, wantHandled) {
			t.Fatalf("handled offsets = %v, want exactly %v", gotHandled, wantHandled)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		wantComm := offSuccessor + 1
		if comm[0] != wantComm {
			t.Fatalf("committed offset = %d, want %d", comm[0], wantComm)
		}

		// Produce next transaction Txn 5 at next offset
		recNext := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-next")}
		if err := txCl.BeginTransaction(); err != nil {
			t.Fatalf("BeginTransaction 5: %v", err)
		}
		if err := txCl.ProduceSync(sctx, recNext).FirstErr(); err != nil {
			t.Fatalf("ProduceSync 5: %v", err)
		}
		if err := txCl.EndTransaction(sctx, kgo.TryCommit); err != nil {
			t.Fatalf("EndTransaction 5: %v", err)
		}
		offNext := recNext.Offset

		// Fresh consumer resumes with the same group
		var fmu sync.Mutex
		var freshHandled []int64
		freshCfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			ClientOpts: []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())},
			Handler: func(_ context.Context, r *kgo.Record) error {
				fmu.Lock()
				freshHandled = append(freshHandled, r.Offset)
				fmu.Unlock()
				return nil
			},
		}
		fctx, fcancel := context.WithCancel(sctx)
		fr := start(t, fctx, freshCfg)
		fr.until(t, "fresh consumer handled new record", func() bool {
			fmu.Lock()
			n := len(freshHandled)
			fmu.Unlock()
			return n >= 1
		})
		fcancel()
		if err := fr.wait(t, waitFor); err != nil {
			t.Fatalf("fresh Run: %v", err)
		}

		fmu.Lock()
		defer fmu.Unlock()
		if len(freshHandled) != 1 || freshHandled[0] != offNext {
			t.Fatalf("fresh consumer saw offsets %v, want exactly [%d] (no re-delivery, no skip)", freshHandled, offNext)
		}
	})
}

// Case 7b: Transactional source topic with default isolation (READ_UNCOMMITTED).
// CONTRACT.md: Guarantee 3 (settlement), Guarantee 4 (commit never past unsettled).
// Review verdict: fix (derive offsets dynamically from produce results/ack to support all brokers including Redpanda).
func TestEdgeOffsetGaps_TransactionalSourceTopic_DefaultIsolation(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	txCl := testClient(t, e.brokers,
		kgo.TransactionalID("tx-def-"+testenv.Group(t)),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Txn 1: Commit record 0 ("tx-0")
	rec0 := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-0")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 1: %v", err)
	}
	if err := txCl.ProduceSync(ctx, rec0).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 1: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 1: %v", err)
	}
	off0 := rec0.Offset

	// Txn 2: Abort record ("tx-aborted") -> visible under default isolation
	recAbort := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-aborted")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recAbort).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 2: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction 2: %v", err)
	}
	offAbort := recAbort.Offset

	// Txn 3: Commit record ("tx-interior") -> interior failing record
	recInterior := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-interior")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 3: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recInterior).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 3: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 3: %v", err)
	}
	offInterior := recInterior.Offset

	// Txn 4: Commit record ("tx-successor") -> visible successor
	recSuccessor := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-successor")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 4: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recSuccessor).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 4: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 4: %v", err)
	}
	offSuccessor := recSuccessor.Offset

	// Subcase 1: Failing record at offInterior blocks commit, successor never entered
	t.Run("FailBlocksCommit", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var seenOffsets []int64
		var seenValues []string
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			MaxAttempts: 1,
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				seenOffsets = append(seenOffsets, r.Offset)
				seenValues = append(seenValues, string(r.Value))
				mu.Unlock()
				if r.Offset == offInterior {
					return errBoom
				}
				return nil
			},
		}
		err := offsetkeeper.Run(sctx, cfg)
		if err == nil {
			t.Fatal("expected error from Run on interior failure")
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run error = %v, want errors.Is errBoom", err)
		}

		mu.Lock()
		gotSeen := slices.Clone(seenOffsets)
		gotVals := slices.Clone(seenValues)
		mu.Unlock()

		// Under default isolation, records [off0, offAbort, offInterior] must be seen; offSuccessor must NOT
		wantSeen := []int64{off0, offAbort, offInterior}
		if !slices.Equal(gotSeen, wantSeen) {
			t.Fatalf("handler seen offsets = %v, want exactly %v", gotSeen, wantSeen)
		}
		if len(gotVals) < 2 || gotVals[1] != "tx-aborted" {
			t.Fatalf("handler did not see aborted payload: %v", gotVals)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		// Offsets off0 and offAbort settled; commit must be offAbort + 1
		wantComm := offAbort + 1
		if comm[0] != wantComm {
			t.Fatalf("committed = %d, want %d (offAbort + 1)", comm[0], wantComm)
		}

		freshCl := testClient(t, e.brokers,
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(e.topic),
		)
		var resumedOffs []int64
		testenv.Eventually(t, waitFor, func() bool {
			fs := freshCl.PollFetches(sctx)
			if err := fs.Err0(); err != nil {
				t.Fatalf("fetch error: %v", err)
			}
			fs.EachRecord(func(r *kgo.Record) {
				resumedOffs = append(resumedOffs, r.Offset)
			})
			return len(resumedOffs) >= 2
		}, fmt.Sprintf("fresh consumer to read [%d, %d]", offInterior, offSuccessor))

		wantResumed := []int64{offInterior, offSuccessor}
		if !slices.Equal(resumedOffs, wantResumed) {
			t.Fatalf("fresh consumer resumed offsets = %v, want %v", resumedOffs, wantResumed)
		}
	})

	// Subcase 2: Fully settled batch asserts [off0, offAbort, offInterior, offSuccessor]
	t.Run("FullySettledResumesClean", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var handled []int64
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				handled = append(handled, r.Offset)
				mu.Unlock()
				return nil
			},
		}
		rctx, rcancel := context.WithCancel(sctx)
		r := start(t, rctx, cfg)
		r.until(t, "handled all 4 visible records", func() bool {
			mu.Lock()
			n := len(handled)
			mu.Unlock()
			return n >= 4
		})
		rcancel()
		if err := r.wait(t, waitFor); err != nil {
			t.Fatalf("Run: %v", err)
		}

		mu.Lock()
		gotHandled := slices.Clone(handled)
		mu.Unlock()

		wantHandled := []int64{off0, offAbort, offInterior, offSuccessor}
		if !slices.Equal(gotHandled, wantHandled) {
			t.Fatalf("handled offsets = %v, want exactly %v", gotHandled, wantHandled)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		wantComm := offSuccessor + 1
		if comm[0] != wantComm {
			t.Fatalf("committed offset = %d, want %d", comm[0], wantComm)
		}

		// Produce next record Txn 5
		recNext := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-next")}
		if err := txCl.BeginTransaction(); err != nil {
			t.Fatalf("BeginTransaction 5: %v", err)
		}
		if err := txCl.ProduceSync(sctx, recNext).FirstErr(); err != nil {
			t.Fatalf("ProduceSync 5: %v", err)
		}
		if err := txCl.EndTransaction(sctx, kgo.TryCommit); err != nil {
			t.Fatalf("EndTransaction 5: %v", err)
		}
		offNext := recNext.Offset

		var fmu sync.Mutex
		var freshHandled []int64
		freshCfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			Handler: func(_ context.Context, r *kgo.Record) error {
				fmu.Lock()
				freshHandled = append(freshHandled, r.Offset)
				fmu.Unlock()
				return nil
			},
		}
		fctx, fcancel := context.WithCancel(sctx)
		fr := start(t, fctx, freshCfg)
		fr.until(t, "fresh consumer handled new record", func() bool {
			fmu.Lock()
			n := len(freshHandled)
			fmu.Unlock()
			return n >= 1
		})
		fcancel()
		if err := fr.wait(t, waitFor); err != nil {
			t.Fatalf("fresh Run: %v", err)
		}

		fmu.Lock()
		defer fmu.Unlock()
		if len(freshHandled) != 1 || freshHandled[0] != offNext {
			t.Fatalf("fresh consumer saw offsets %v, want exactly [%d]", freshHandled, offNext)
		}
	})
}

// Case 8: Offset gaps from log truncation / retention (kadm.DeleteRecords; start offset > 0).
// CONTRACT.md: Guarantee 4 ("highest contiguous settled offset per partition (offset plus one). Never past an unsettled record").
// Review verdict: fix (check response.Error(), verify low watermark 3, assert [3,4] on failure and [3,4,5] on success, make failure interior).
func TestEdgeOffsetGaps_LogTruncationDeletedRecords(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	admCl := testClient(t, e.brokers)
	adm := kadm.NewClient(admCl)

	// Produce 6 records (offsets 0..5)
	var rs []*kgo.Record
	for i := 0; i < 6; i++ {
		rs = append(rs, &kgo.Record{
			Topic: e.topic, Partition: 0,
			Value: []byte(fmt.Sprintf("v-%d", i)),
		})
	}
	testenv.Produce(t, e.brokers, rs...)

	// Truncate records before offset 3
	delOffsets := make(kadm.Offsets)
	delOffsets.AddOffset(e.topic, 0, 3, -1)
	delResp, err := adm.DeleteRecords(ctx, delOffsets)
	if err != nil {
		t.Fatalf("DeleteRecords: %v", err)
	}
	if err := delResp.Error(); err != nil {
		t.Fatalf("DeleteRecords response error: %v", err)
	}

	// Verify low watermark is 3 via ListStartOffsets
	startOffs, err := adm.ListStartOffsets(ctx, e.topic)
	if err != nil {
		t.Fatalf("ListStartOffsets: %v", err)
	}
	if low := startOffs[e.topic][0].Offset; low != 3 {
		t.Fatalf("low watermark offset = %d, want 3", low)
	}

	// Subcase 1: Failing record at offset 4 after gap (start offset = 3, visible successor is 5)
	t.Run("FailAfterGapBlocksCommit", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var seenOffsets []int64
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			MaxAttempts: 1,
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				seenOffsets = append(seenOffsets, r.Offset)
				mu.Unlock()
				if r.Offset == 4 {
					return errBoom
				}
				return nil
			},
		}
		err := offsetkeeper.Run(sctx, cfg)
		if err == nil {
			t.Fatal("expected error from Run")
		}
		if !errors.Is(err, errBoom) {
			t.Fatalf("Run error = %v, want errors.Is errBoom", err)
		}

		mu.Lock()
		gotSeen := slices.Clone(seenOffsets)
		mu.Unlock()

		// Handler trace must be exactly [3, 4]; successor 5 must never run
		wantSeen := []int64{3, 4}
		if !slices.Equal(gotSeen, wantSeen) {
			t.Fatalf("handler seen offsets = %v, want exactly %v", gotSeen, wantSeen)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		// Offset 3 was settled, so committed offset is 4 (never past 4)
		if comm[0] != 4 {
			t.Fatalf("committed offset = %d, want 4", comm[0])
		}

		// Fresh consumer resuming sees records [4, 5]
		freshCl := testClient(t, e.brokers, kgo.ConsumerGroup(group), kgo.ConsumeTopics(e.topic))
		var resumedOffs []int64
		testenv.Eventually(t, waitFor, func() bool {
			fs := freshCl.PollFetches(sctx)
			if err := fs.Err0(); err != nil {
				t.Fatalf("fetch error: %v", err)
			}
			fs.EachRecord(func(r *kgo.Record) { resumedOffs = append(resumedOffs, r.Offset) })
			return len(resumedOffs) >= 2
		}, "fresh consumer to read [4, 5]")

		wantResumed := []int64{4, 5}
		if !slices.Equal(resumedOffs, wantResumed) {
			t.Fatalf("resumed offsets = %v, want %v", resumedOffs, wantResumed)
		}
	})

	// Subcase 2: Fully settled batch asserts [3, 4, 5]
	t.Run("FullySettled", func(t *testing.T) {
		sctx, scancel := context.WithTimeout(ctx, waitFor)
		defer scancel()
		group := testenv.Group(t)

		var mu sync.Mutex
		var handled []int64
		cfg := offsetkeeper.Config{
			Brokers: e.brokers, Group: group, Topics: []string{e.topic},
			Handler: func(_ context.Context, r *kgo.Record) error {
				mu.Lock()
				handled = append(handled, r.Offset)
				mu.Unlock()
				return nil
			},
		}
		rctx, rcancel := context.WithCancel(sctx)
		r := start(t, rctx, cfg)
		r.until(t, "handled records 3, 4, 5", func() bool {
			mu.Lock()
			n := len(handled)
			mu.Unlock()
			return n >= 3
		})
		rcancel()
		if err := r.wait(t, waitFor); err != nil {
			t.Fatalf("Run: %v", err)
		}

		mu.Lock()
		gotHandled := slices.Clone(handled)
		mu.Unlock()

		wantHandled := []int64{3, 4, 5}
		if !slices.Equal(gotHandled, wantHandled) {
			t.Fatalf("handled offsets = %v, want exactly %v", gotHandled, wantHandled)
		}

		comm := testenv.Committed(t, e.brokers, group, e.topic)
		if comm[0] != 6 {
			t.Fatalf("committed offset = %d, want 6 (end offset)", comm[0])
		}
	})
}

// Case 9: Records with LeaderEpoch set; handler mutation does not affect broker commit.
// CONTRACT.md: Guarantee 4 ("Handlers cannot change the committed offset by modifying their record").
// Review verdict: fix (compare kadm.FetchOffsets partition LeaderEpoch with captured epoch as well as At).
func TestEdgeRecords_LeaderEpoch(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	e.produce(t, "le", 3)

	var sawEpoch atomic.Int32
	sawEpoch.Store(-1)
	cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
		// Capture broker's genuine leader epoch on last record
		sawEpoch.Store(r.LeaderEpoch)
		// Tamper with LeaderEpoch in handler copy
		r.LeaderEpoch = 9999
		return nil
	})

	r := start(t, ctx, cfg)
	r.until(t, "handled records", func() bool { return sawEpoch.Load() >= 0 && e.drained(t) })
	r.cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run: %v", err)
	}

	capturedEpoch := sawEpoch.Load()
	if capturedEpoch < 0 {
		t.Fatalf("captured leader epoch = %d, want >= 0", capturedEpoch)
	}

	// Fetch raw offset response including LeaderEpoch via kadm
	admCl := testClient(t, e.brokers)
	resp, err := kadm.NewClient(admCl).FetchOffsets(ctx, e.group)
	if err != nil {
		t.Fatalf("FetchOffsets: %v", err)
	}
	partOff, ok := resp[e.topic][0]
	if !ok {
		t.Fatalf("topic %s/0 missing from FetchOffsets response", e.topic)
	}
	if partOff.Err != nil {
		t.Fatalf("fetch offset error: %v", partOff.Err)
	}
	if partOff.At != 3 {
		t.Fatalf("committed At = %d, want 3", partOff.At)
	}
	if partOff.LeaderEpoch != capturedEpoch {
		t.Fatalf("committed LeaderEpoch = %d, want captured broker epoch %d (tampered 9999 must not be committed)", partOff.LeaderEpoch, capturedEpoch)
	}
}

// Case 10: A batch where the last record of a partition is the failing one.
// CONTRACT.md: Guarantee 3 (settlement), Guarantee 4 (commit prefix), Guarantee 7 (DLQ).
// Review verdict: keep (identify DLQ source offset 4 for dead letter).
func TestEdgeRecords_BatchLastRecordFails(t *testing.T) {
	// Subcase A: Without DLQ
	t.Run("NoDLQ", func(t *testing.T) {
		e, ctx := newEnv(t, 1), testCtx(t)
		e.produce(t, "last", 5) // offsets 0, 1, 2, 3, 4

		cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
			if r.Offset == 4 { // last record in partition
				return errBoom
			}
			return nil
		})
		cfg.MaxAttempts = 1

		err := offsetkeeper.Run(ctx, cfg)
		if err == nil {
			t.Fatal("expected error from Run on last record")
		}
		mustContain(t, "Run error", err.Error(), e.topic, "offset 4")

		comm := testenv.Committed(t, e.brokers, e.group, e.topic)
		// Settled prefix was offsets 0..3, so committed offset must be 4
		if comm[0] != 4 {
			t.Fatalf("committed offset = %d, want 4", comm[0])
		}

		// Fresh consumer receives the failed record
		freshCl := testClient(t, e.brokers, kgo.ConsumerGroup(e.group), kgo.ConsumeTopics(e.topic))
		fs := freshCl.PollFetches(ctx)
		var resumed []int64
		fs.EachRecord(func(r *kgo.Record) { resumed = append(resumed, r.Offset) })
		if len(resumed) != 1 || resumed[0] != 4 {
			t.Fatalf("fresh consumer resumed = %v, want [4]", resumed)
		}
	})

	// Subcase B: With DLQ
	t.Run("WithDLQ", func(t *testing.T) {
		e, ctx := newEnv(t, 1), testCtx(t)
		dlqTopic := testenv.Topic(t, e.brokers, 1)
		e.produce(t, "last-dlq", 5) // offsets 0..4

		cfg := e.cfg(func(_ context.Context, r *kgo.Record) error {
			if r.Offset == 4 {
				return errBoom
			}
			return nil
		})
		cfg.DLQTopic = dlqTopic
		cfg.MaxAttempts = 1

		r := start(t, ctx, cfg)
		r.until(t, "last record dead-lettered and batch drained", func() bool {
			return e.total(t, dlqTopic) == 1 && e.drained(t)
		})
		r.cancel()
		if err := r.wait(t, waitFor); err != nil {
			t.Fatalf("Run: %v", err)
		}

		comm := testenv.Committed(t, e.brokers, e.group, e.topic)
		if comm[0] != 5 {
			t.Fatalf("committed offset = %d, want 5", comm[0])
		}

		// Identify DLQ source offset 4
		dlqRecs := testenv.Records(t, e.brokers, dlqTopic)
		if len(dlqRecs) != 1 {
			t.Fatalf("DLQ records count = %d, want 1", len(dlqRecs))
		}
		if offStr, ok := header(dlqRecs[0], dlq.HeaderSourceOffset); !ok || offStr != "4" {
			t.Fatalf("DLQ record HeaderSourceOffset = %q (ok=%v), want 4", offStr, ok)
		}
	})
}

// Case 12: 0-record topics (empty topic).
// CONTRACT.md: Guarantee 6 (Shutdown). Docs are silent on empty topics; safe behavior: no hang, no panic, clean return on cancel.
// Review verdict: fix (wait on an assignment or first Fetch for this topic before cancel).
func TestEdgeRecords_ZeroRecordTopic(t *testing.T) {
	e, ctx := newEnv(t, 3), testCtx(t) // 3 empty partitions

	var mu sync.Mutex
	var callCount int
	assigned := make(chan struct{})
	var assignedOnce sync.Once

	cfg := e.cfg(func(_ context.Context, _ *kgo.Record) error {
		mu.Lock()
		callCount++
		mu.Unlock()
		return nil
	})
	cfg.ClientOpts = []kgo.Opt{
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, parts map[string][]int32) {
			if len(parts[e.topic]) > 0 {
				assignedOnce.Do(func() { close(assigned) })
			}
		}),
	}

	rctx, cancel := context.WithCancel(ctx)
	r := start(t, rctx, cfg)

	// Wait until assignment is active on this topic before cancelling
	select {
	case <-assigned:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for topic partition assignment")
	}

	cancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("Run returned error on 0-record topic: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if callCount != 0 {
		t.Fatalf("handler called %d times on empty topic", callCount)
	}

	comm := testenv.Committed(t, e.brokers, e.group, e.topic)
	if len(comm) != 0 {
		t.Fatalf("committed offsets = %v on empty topic, want none", comm)
	}
}

// Missing Case (a): Two configured source topics with the same partition number and overlapping offsets.
// Fail one topic, verify separate handler identities, settled prefixes and group checkpoints.
// CONTRACT.md: Guarantee 2 (order per partition), Guarantee 4 (commit prefix per partition).
func TestEdgeRecords_TwoSourceTopicsSamePartitionFailOne(t *testing.T) {
	b := testenv.Brokers(t)
	topicA := testenv.Topic(t, b, 1)
	topicB := testenv.Topic(t, b, 1)
	group := testenv.Group(t)
	ctx := testCtx(t)

	var mu sync.Mutex
	type recordID struct {
		topic  string
		offset int64
		value  string
	}
	var handled []recordID

	cfg := offsetkeeper.Config{
		Brokers: b, Group: group, Topics: []string{topicA, topicB},
		MaxAttempts: 1,
		Handler: func(_ context.Context, r *kgo.Record) error {
			mu.Lock()
			handled = append(handled, recordID{r.Topic, r.Offset, string(r.Value)})
			mu.Unlock()
			// Fail topicA on offset 1; all other records succeed
			if r.Topic == topicA && r.Offset == 1 {
				return errBoom
			}
			return nil
		},
	}

	// Start runner
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	r := start(t, rctx, cfg)

	// Wave 1: Produce A-0 to topicA and B-0, B-1, B-2 to topicB (overlapping offset 0)
	testenv.Produce(t, b, &kgo.Record{Topic: topicA, Partition: 0, Value: []byte("A-0")})
	var rsB []*kgo.Record
	for i := 0; i < 3; i++ {
		rsB = append(rsB, &kgo.Record{Topic: topicB, Partition: 0, Value: []byte(fmt.Sprintf("B-%d", i))})
	}
	testenv.Produce(t, b, rsB...)

	// Wait until Wave 1 is fully handled and committed
	r.until(t, "Wave 1 handled", func() bool {
		mu.Lock()
		defer mu.Unlock()
		var countA, countB int
		for _, h := range handled {
			switch h.topic {
			case topicA:
				countA++
			case topicB:
				countB++
			}
		}
		return countA >= 1 && countB >= 3
	})

	// Wave 2: Produce A-1 (fails) and A-2 (visible successor) to topicA
	testenv.Produce(t, b,
		&kgo.Record{Topic: topicA, Partition: 0, Value: []byte("A-1")},
		&kgo.Record{Topic: topicA, Partition: 0, Value: []byte("A-2")},
	)

	// Run must exit with errBoom from topicA
	err := r.wait(t, waitFor)
	if err == nil {
		t.Fatal("expected error from Run on topicA failure")
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run error = %v, want errors.Is errBoom", err)
	}

	mu.Lock()
	gotHandled := slices.Clone(handled)
	mu.Unlock()

	// Verify separate handler identities: records delivered with their own Topic field
	var seenA, seenB []int64
	for _, h := range gotHandled {
		switch h.topic {
		case topicA:
			seenA = append(seenA, h.offset)
		case topicB:
			seenB = append(seenB, h.offset)
		}
	}
	wantSeenA := []int64{0, 1}
	if !slices.Equal(seenA, wantSeenA) {
		t.Fatalf("topicA seen offsets = %v, want exactly %v", seenA, wantSeenA)
	}
	wantSeenB := []int64{0, 1, 2}
	if !slices.Equal(seenB, wantSeenB) {
		t.Fatalf("topicB seen offsets = %v, want exactly %v", seenB, wantSeenB)
	}

	// Verify group checkpoints per topic:
	// topicA committed offset must be 1 (prefix offset 0 + 1; not past failed offset 1)
	commA := testenv.Committed(t, b, group, topicA)
	if commA[0] != 1 {
		t.Fatalf("topicA committed offset = %d, want 1", commA[0])
	}
	// topicB was fully settled, committed offset must be 3
	commB := testenv.Committed(t, b, group, topicB)
	if commB[0] != 3 {
		t.Fatalf("topicB committed offset = %d, want 3", commB[0])
	}

	// Fresh consumer resuming on both topics: receives only uncommitted topicA records (1, 2)
	freshCl := testClient(t, b, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topicA, topicB))
	var freshA, freshB []string
	testenv.Eventually(t, waitFor, func() bool {
		fs := freshCl.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatalf("fresh fetch error: %v", err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Topic == topicA {
				freshA = append(freshA, string(r.Value))
			} else {
				freshB = append(freshB, string(r.Value))
			}
		})
		return len(freshA) >= 2
	}, "fresh consumer to read uncommitted topicA records")

	if len(freshB) != 0 {
		t.Fatalf("fresh consumer received already-committed topicB records: %v", freshB)
	}
	wantFreshA := []string{"A-1", "A-2"}
	if !slices.Equal(freshA, wantFreshA) {
		t.Fatalf("fresh consumer topicA records = %v, want %v", freshA, wantFreshA)
	}
}

// Missing Case (b): An interior transactional failure followed by another visible record in the same partition and batch.
// Assert that the handler never sees the successor before restart, then verify both redeliver in order.
// CONTRACT.md: Guarantee 2 (order per partition), Guarantee 3 (settlement), Guarantee 4 (commit).
func TestEdgeRecords_InteriorTransactionalFailure(t *testing.T) {
	e, ctx := newEnv(t, 1), testCtx(t)
	txCl := testClient(t, e.brokers,
		kgo.TransactionalID("tx-int-"+testenv.Group(t)),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)

	// Produce 4 transactions to partition 0:
	// Tx 1: Commit ("tx-0")
	rec0 := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-0")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 1: %v", err)
	}
	if err := txCl.ProduceSync(ctx, rec0).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 1: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 1: %v", err)
	}
	off0 := rec0.Offset

	// Tx 2: Abort ("tx-aborted") -> control markers / gap
	recAbort := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-aborted")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 2: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recAbort).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 2: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction 2: %v", err)
	}

	// Tx 3: Commit ("tx-4-fail") -> interior failing record
	recFail := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-4-fail")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 3: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recFail).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 3: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 3: %v", err)
	}

	// Tx 4: Commit ("tx-6-successor") -> visible successor in same partition/batch
	recSuccessor := &kgo.Record{Topic: e.topic, Partition: 0, Value: []byte("tx-6-successor")}
	if err := txCl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction 4: %v", err)
	}
	if err := txCl.ProduceSync(ctx, recSuccessor).FirstErr(); err != nil {
		t.Fatalf("ProduceSync 4: %v", err)
	}
	if err := txCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction 4: %v", err)
	}
	offSuccessor := recSuccessor.Offset

	group := testenv.Group(t)
	var mu sync.Mutex
	var seenValues []string
	cfg := offsetkeeper.Config{
		Brokers: e.brokers, Group: group, Topics: []string{e.topic},
		MaxAttempts: 1,
		ClientOpts:  []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())},
		Handler: func(_ context.Context, r *kgo.Record) error {
			mu.Lock()
			seenValues = append(seenValues, string(r.Value))
			mu.Unlock()
			if string(r.Value) == "tx-4-fail" {
				return errBoom
			}
			return nil
		},
	}

	// First Run: fails on interior record tx-4-fail
	err := offsetkeeper.Run(ctx, cfg)
	if err == nil {
		t.Fatal("expected error from Run on interior failure")
	}
	if !errors.Is(err, errBoom) {
		t.Fatalf("Run error = %v, want errors.Is errBoom", err)
	}

	mu.Lock()
	gotFirstRun := slices.Clone(seenValues)
	mu.Unlock()

	// Handler must never have seen successor tx-6-successor before restart
	wantFirstRun := []string{"tx-0", "tx-4-fail"}
	if !slices.Equal(gotFirstRun, wantFirstRun) {
		t.Fatalf("first run saw = %v, want exactly %v (successor must never run)", gotFirstRun, wantFirstRun)
	}

	// Settle prefix was tx-0 (offset off0), so committed offset is off0 + 1
	comm := testenv.Committed(t, e.brokers, group, e.topic)
	wantComm := off0 + 1
	if comm[0] != wantComm {
		t.Fatalf("committed offset = %d, want %d", comm[0], wantComm)
	}

	// Restarted Run (success handler): both redeliver in order [tx-4-fail, tx-6-successor]
	var restartMu sync.Mutex
	var restartSeen []string
	restartCfg := offsetkeeper.Config{
		Brokers: e.brokers, Group: group, Topics: []string{e.topic},
		ClientOpts: []kgo.Opt{kgo.FetchIsolationLevel(kgo.ReadCommitted())},
		Handler: func(_ context.Context, r *kgo.Record) error {
			restartMu.Lock()
			restartSeen = append(restartSeen, string(r.Value))
			restartMu.Unlock()
			return nil
		},
	}
	rctx, rcancel := context.WithCancel(ctx)
	r := start(t, rctx, restartCfg)
	r.until(t, "both redelivered in order", func() bool {
		restartMu.Lock()
		n := len(restartSeen)
		restartMu.Unlock()
		return n >= 2
	})
	rcancel()
	if err := r.wait(t, waitFor); err != nil {
		t.Fatalf("restart Run: %v", err)
	}

	restartMu.Lock()
	gotRestart := slices.Clone(restartSeen)
	restartMu.Unlock()

	wantRestart := []string{"tx-4-fail", "tx-6-successor"}
	if !slices.Equal(gotRestart, wantRestart) {
		t.Fatalf("restarted run saw = %v, want exactly %v", gotRestart, wantRestart)
	}

	// Verify final checkpoint after full settlement is offSuccessor + 1
	finalComm := testenv.Committed(t, e.brokers, group, e.topic)
	wantFinalComm := offSuccessor + 1
	if finalComm[0] != wantFinalComm {
		t.Fatalf("final committed offset = %d, want %d", finalComm[0], wantFinalComm)
	}
}
