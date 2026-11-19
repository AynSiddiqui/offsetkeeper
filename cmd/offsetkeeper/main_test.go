package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestUsageAndFlagValidation(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		args []string
	}{
		{"no args", []string{}},
		{"help flag -h", []string{"-h"}},
		{"help flag --help", []string{"--help"}},
		{"unknown command", []string{"unknown"}},
		{"dlq without subcommand", []string{"dlq"}},
		{"dlq -h", []string{"dlq", "-h"}},
		{"dlq unknown subcommand", []string{"dlq", "unknown"}},

		// dlq list flag errors
		{"list missing all flags", []string{"dlq", "list"}},
		{"list missing topic", []string{"dlq", "list", "-brokers", "localhost:9092"}},
		{"list missing brokers", []string{"dlq", "list", "-topic", "my-dlq"}},
		{"list empty brokers", []string{"dlq", "list", "-brokers", "", "-topic", "my-dlq"}},
		{"list empty topic", []string{"dlq", "list", "-brokers", "localhost:9092", "-topic", ""}},
		{"list extra args", []string{"dlq", "list", "-brokers", "localhost:9092", "-topic", "my-dlq", "extra"}},
		{"list unknown flag", []string{"dlq", "list", "-unknown"}},

		// dlq replay flag errors
		{"replay missing all flags", []string{"dlq", "replay"}},
		{"replay missing topic", []string{"dlq", "replay", "-brokers", "localhost:9092"}},
		{"replay missing brokers", []string{"dlq", "replay", "-topic", "my-dlq"}},
		{"replay empty brokers", []string{"dlq", "replay", "-brokers", "", "-topic", "my-dlq"}},
		{"replay empty topic", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", ""}},
		{"replay partition without offset", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "-partition", "0"}},
		{"replay offset without partition", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "-offset", "0"}},
		{"replay negative partition", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "-partition", "-1", "-offset", "0"}},
		{"replay negative offset", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "-partition", "0", "-offset", "-1"}},
		{"replay extra args", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "extra"}},
		{"replay unknown flag", []string{"dlq", "replay", "-unknown"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(ctx, tc.args, &stdout, &stderr)
			if code != 2 {
				t.Fatalf("run(%v) = %d, want exit code 2 (usage/flag error)", tc.args, code)
			}
			if stderr.Len() == 0 {
				t.Fatalf("run(%v) produced empty stderr, want usage message", tc.args)
			}
		})
	}
}

func TestDLQListIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)

	src := &kgo.Record{
		Topic:     "source-topic-1",
		Partition: 0,
		Offset:    42,
		Key:       []byte("test-key"),
		Value:     []byte("test-val"),
	}
	hs := dlq.Headers(src, 3, errors.New("something went wrong"))
	testenv.Produce(t, brokers, &kgo.Record{
		Topic:   topic,
		Key:     src.Key,
		Value:   src.Value,
		Headers: hs,
	})

	var stdout, stderr bytes.Buffer
	code := run(ctx, []string{
		"dlq", "list",
		"-brokers", strings.Join(brokers, ","),
		"-topic", topic,
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("run dlq list failed with code %d; stderr: %s", code, stderr.String())
	}

	out := stdout.String()
	wantHeader := "partition\toffset\tsource_topic\tsource_partition\tsource_offset\tattempts\terror\n"
	if !strings.HasPrefix(out, wantHeader) {
		t.Fatalf("stdout does not start with expected header:\ngot:  %q\nwant: %q", out, wantHeader)
	}

	wantEntry := "0\t0\tsource-topic-1\t0\t42\t3\tsomething went wrong\n"
	if !strings.Contains(out, wantEntry) {
		t.Fatalf("stdout does not contain expected entry:\ngot:  %q\nwant: %q", out, wantEntry)
	}
}

func TestDLQReplayIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	brokers := testenv.Brokers(t)
	srcTopic := testenv.Topic(t, brokers, 1)
	dlqTopic := testenv.Topic(t, brokers, 1)

	src1 := &kgo.Record{
		Topic:     srcTopic,
		Partition: 0,
		Offset:    10,
		Key:       []byte("k1"),
		Value:     []byte("v1"),
	}
	testenv.Produce(t, brokers, &kgo.Record{
		Topic:   dlqTopic,
		Key:     src1.Key,
		Value:   src1.Value,
		Headers: dlq.Headers(src1, 2, errors.New("err1")),
	})

	// ReplayOne test
	var stdout, stderr bytes.Buffer
	code := run(ctx, []string{
		"dlq", "replay",
		"-brokers", strings.Join(brokers, ","),
		"-topic", dlqTopic,
		"-partition", "0",
		"-offset", "0",
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("run dlq replay one failed with code %d; stderr: %s", code, stderr.String())
	}
	want := "replayed 1 record(s)\n"
	if stdout.String() != want {
		t.Fatalf("run dlq replay one output = %q, want %q", stdout.String(), want)
	}

	// Verify record reached srcTopic
	recs := testenv.Records(t, brokers, srcTopic)
	if len(recs) != 1 || string(recs[0].Key) != "k1" {
		t.Fatalf("expected 1 replayed record on %s, got %v", srcTopic, recs)
	}

	// ReplayAll test
	src2 := &kgo.Record{
		Topic:     srcTopic,
		Partition: 0,
		Offset:    11,
		Key:       []byte("k2"),
		Value:     []byte("v2"),
	}
	testenv.Produce(t, brokers, &kgo.Record{
		Topic:   dlqTopic,
		Key:     src2.Key,
		Value:   src2.Value,
		Headers: dlq.Headers(src2, 1, errors.New("err2")),
	})

	stdout.Reset()
	stderr.Reset()
	code = run(ctx, []string{
		"dlq", "replay",
		"-brokers", strings.Join(brokers, ","),
		"-topic", dlqTopic,
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("run dlq replay all failed with code %d; stderr: %s", code, stderr.String())
	}
	out := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(out, "replayed ") || !strings.HasSuffix(out, " record(s)") {
		t.Fatalf("run dlq replay all output = %q, want prefix 'replayed ' and suffix ' record(s)'", out)
	}
	if out == "replayed 0 record(s)" {
		t.Fatalf("run dlq replay all output = %q, want > 0 records", out)
	}
}
