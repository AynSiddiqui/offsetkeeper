package main

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestEdgeCLIBrokersWhitespaceAndCommas tests that -brokers containing only
// commas and spaces is rejected with code 2 and a message about at least one broker address.
// Doc ref: cmd/offsetkeeper/main.go validateCommon; CONTRACT.md is silent on CLI flag parsing.
func TestEdgeCLIBrokersWhitespaceAndCommas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	badBrokers := []string{
		" , ",
		" , , , ",
		"   ",
		",",
		",,,",
	}

	for _, b := range badBrokers {
		t.Run("list: "+b, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(ctx, []string{"dlq", "list", "-brokers", b, "-topic", "some-dlq"}, &stdout, &stderr)
			if code != 2 {
				t.Errorf("run dlq list with -brokers %q = %d, want 2", b, code)
			}
			if !strings.Contains(stderr.String(), "must contain at least one broker address") {
				t.Errorf("stderr = %q, want message about at least one broker address", stderr.String())
			}
		})
		t.Run("replay: "+b, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(ctx, []string{"dlq", "replay", "-brokers", b, "-topic", "some-dlq"}, &stdout, &stderr)
			if code != 2 {
				t.Errorf("run dlq replay with -brokers %q = %d, want 2", b, code)
			}
			if !strings.Contains(stderr.String(), "must contain at least one broker address") {
				t.Errorf("stderr = %q, want message about at least one broker address", stderr.String())
			}
		})
	}
}

// TestEdgeCLITLSAgainstPlaintextKfake tests -tls against an explicit plaintext kfake broker.
// When -tls is specified against a plaintext broker, the ping probe fails fast with an actionable -tls hint.
// Doc ref: cmd/offsetkeeper/main.go newClient; CONTRACT.md is silent on TLS against plaintext.
func TestEdgeCLITLSAgainstPlaintextKfake(t *testing.T) {
	origPing := pingTimeout
	pingTimeout = 1 * time.Second
	t.Cleanup(func() { pingTimeout = origPing })

	// Explicit in-process plaintext kfake cluster
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "dlq"))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	brokers := cluster.ListenAddrs()

	for _, subcmd := range []string{"list", "replay"} {
		t.Run(subcmd, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			start := time.Now()
			var stdout, stderr bytes.Buffer
			args := []string{"dlq", subcmd, "-brokers", strings.Join(brokers, ","), "-topic", "dlq", "-tls"}
			code := run(ctx, args, &stdout, &stderr)
			elapsed := time.Since(start)

			if code != 1 {
				t.Errorf("run(%v) = %d, want exit code 1; stderr: %s", args, code, stderr.String())
			}
			if elapsed >= 8*time.Second || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Errorf("run(%v) hung until context deadline (%v); want fast failure well before 10s; stderr: %s", args, elapsed, stderr.String())
			}
			if stderr.Len() == 0 {
				t.Errorf("run(%v) stderr is empty, want clear error message", args)
			}
			if !strings.Contains(stderr.String(), "-tls") {
				t.Errorf("stderr = %q, want '-tls' hint in stderr", stderr.String())
			}
		})
	}
}

// TestEdgeCLIListLongErrorEscaping tests that listing DLQ records with long error text
// containing control chars, newlines, tabs, and quotes escapes each row onto a single line,
// and that unquoting the field recovers the full original error text byte-for-byte.
// Doc ref: cmd/offsetkeeper/main.go escape and runDLQList; CONTRACT.md §7.
func TestEdgeCLIListLongErrorEscaping(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "dlq-esc"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	brokers := cluster.ListenAddrs()

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 10 KiB error text with raw control characters, newlines, tabs, quotes, backslashes, and multibyte runes
	longErrText := strings.Repeat("line1\nline2\twith\ttabs\rand\r\rcarriage\x1b[31mcolor\x00null\\backslash\"quotes\"-é-🚀 ", 150)
	src := &kgo.Record{
		Topic:     "src-topic-edge",
		Partition: 0,
		Offset:    100,
	}

	// Hand-produce directly with full HeaderError bytes (bypassing Headers 4KiB cap to test CLI escaping)
	rec := &kgo.Record{
		Topic: "dlq-esc",
		Headers: []kgo.RecordHeader{
			{Key: dlq.HeaderSourceTopic, Value: []byte(src.Topic)},
			{Key: dlq.HeaderSourcePartition, Value: []byte("0")},
			{Key: dlq.HeaderSourceOffset, Value: []byte("100")},
			{Key: dlq.HeaderAttempts, Value: []byte("3")},
			{Key: dlq.HeaderError, Value: []byte(longErrText)},
		},
	}
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := run(ctx, []string{"dlq", "list", "-brokers", strings.Join(brokers, ","), "-topic", "dlq-esc"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run dlq list failed = %d, stderr: %s", code, stderr.String())
	}

	out := stdout.String()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected exactly 2 lines (header + 1 record), got %d lines:\n%s", len(lines), out)
	}

	row := lines[1]
	fields := strings.Split(row, "\t")
	if len(fields) != 7 {
		t.Fatalf("row has %d fields, want 7 tab-separated fields: %q", len(fields), row)
	}

	// Verify error field is escaped and does not contain raw newlines, carriage returns, or tabs
	errField := fields[6]
	if strings.ContainsAny(errField, "\n\r\t") {
		t.Errorf("error field contains unescaped control chars: %q", errField)
	}

	// Full exact byte-for-byte comparison of unquoted text against original
	unquoted, err := strconv.Unquote(`"` + errField + `"`)
	if err != nil {
		t.Fatalf("strconv.Unquote on error field failed: %v\nfield: %q", err, errField)
	}
	if unquoted != longErrText {
		t.Errorf("unquoted error text does not match expected length %d (got %d)", len(longErrText), len(unquoted))
	}
}

// FuzzEscape fuzzes the CLI's escape function to ensure:
// 1. Output never contains raw \t, \n, \r.
// 2. Output never contains raw control characters (bytes < 0x20 or 0x7f).
// 3. strconv.Unquote MUST succeed without error and recover s byte-for-byte.
// Doc ref: cmd/offsetkeeper/main.go escape; CONTRACT.md is silent on CLI escaping.
func FuzzEscape(f *testing.F) {
	seeds := []string{
		"",
		"plain text",
		"hello world\n",
		"tab\there\r\n",
		"mixed\t\r\n\x00\x01\x1f\x7fcontrol",
		"quote\"and\\backslash",
		"unicode: 世界 🚀 éè",
		strings.Repeat("a\nb\tc\r", 100),
		"\x1b[31mred\x1b[0m",
		string([]byte{0xff, 0xfe, 0x00}),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		esc := escape(s)

		// 1. Output never contains raw \t, \n, \r
		if strings.ContainsAny(esc, "\t\n\r") {
			t.Fatalf("escape(%q) contains raw tab/newline/cr: %q", s, esc)
		}

		// 2. Output never contains raw control characters (byte < 0x20 or 0x7f)
		for i := 0; i < len(esc); i++ {
			b := esc[i]
			if b < 0x20 || b == 0x7f {
				t.Fatalf("escape(%q) contains raw control byte 0x%02x at index %d: %q", s, b, i, esc)
			}
		}

		// 3. Unquote MUST succeed on any escape output and recover s exactly
		quoted := `"` + esc + `"`
		unquoted, err := strconv.Unquote(quoted)
		if err != nil {
			t.Fatalf("strconv.Unquote(%q) failed: %v; input was %q", quoted, err, s)
		}
		if unquoted != s {
			t.Fatalf("unquoted (%d bytes) != original s (%d bytes)", len(unquoted), len(s))
		}
	})
}
