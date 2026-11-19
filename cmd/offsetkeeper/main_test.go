package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/AynSiddiqui/offsetkeeper/internal/testenv"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestUsageAndFlagValidation(t *testing.T) {
	// Bounded so a missing validation (the CLI would try to connect) fails instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

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
		{"replay partition overflow", []string{"dlq", "replay", "-brokers", "localhost:9092", "-topic", "my-dlq", "-partition", "4294967296", "-offset", "0"}},
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

func TestEscape(t *testing.T) {
	for in, want := range map[string]string{
		"plain text":   "plain text",
		"a\nb\tc":      `a\nb\tc`,
		"back\\slash":  `back\\slash`,
		"\x1b[31mred":  `\x1b[31mred`,
		"héllo":        "héllo",
		"say \"hi\"\r": `say \"hi\"\r`,
	} {
		if got := escape(in); got != want {
			t.Errorf("escape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDLQListEscapesFreeText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := testenv.Brokers(t)
	topic := testenv.Topic(t, brokers, 1)
	src := &kgo.Record{Topic: "src\ttopic", Partition: 0, Offset: 7}
	testenv.Produce(t, brokers, &kgo.Record{Topic: topic, Headers: dlq.Headers(src, 2, errors.New("a\nb\tc"))})

	var stdout, stderr bytes.Buffer
	args := []string{"dlq", "list", "-brokers", strings.Join(brokers, ","), "-topic", topic}
	if code := run(ctx, args, &stdout, &stderr); code != 0 {
		t.Fatalf("run dlq list = %d; stderr: %s", code, stderr.String())
	}
	lines := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("output has %d lines, want header + 1 row: %q", len(lines), stdout.String())
	}
	fields := strings.Split(lines[1], "\t")
	if len(fields) != 7 || fields[2] != `src\ttopic` || fields[6] != `a\nb\tc` {
		t.Errorf("row = %q (%d fields), want 7 fields with source_topic %q and error %q", lines[1], len(fields), `src\ttopic`, `a\nb\tc`)
	}
}

func TestListHelpDocumentsEscaping(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"dlq", "list", "-h"}, &stdout, &stderr); code != 2 {
		t.Fatalf("dlq list -h = %d, want 2", code)
	}
	for _, want := range []string{"escaped", "-tls"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("dlq list -h output does not mention %q:\n%s", want, stderr.String())
		}
	}
}

// selfSignedCA writes a self-signed cert for 127.0.0.1 to a PEM file and returns
// the tls.Config a server needs plus the file's path.
func selfSignedCA(t *testing.T) (*tls.Config, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "offsetkeeper test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true, IsCA: true, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}, path
}

// -tls dials with the system root CAs. Go reads SSL_CERT_FILE once, at the first
// use of the system roots; no other test here uses them, so setting it works.
func TestTLSFlag(t *testing.T) {
	serverCfg, caFile := selfSignedCA(t)
	t.Setenv("SSL_CERT_FILE", caFile)
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(serverCfg), kfake.SeedTopics(1, "dlq"))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	brokers := strings.Join(cluster.ListenAddrs(), ",")

	for _, args := range [][]string{
		{"dlq", "list", "-brokers", brokers, "-topic", "dlq", "-tls"},
		{"dlq", "replay", "-brokers", brokers, "-topic", "dlq", "-tls"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		var stdout, stderr bytes.Buffer
		if code := run(ctx, args, &stdout, &stderr); code != 0 {
			t.Errorf("run %v = %d, want 0; stderr: %s", args, code, stderr.String())
		}
		cancel()
	}
}
