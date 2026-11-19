// Command offsetkeeper provides CLI tools for managing dead-letter queues.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AynSiddiqui/offsetkeeper/internal/dlq"
	"github.com/twmb/franz-go/pkg/kgo"
)

// pingTimeout bounds the initial broker reachability probe; var so tests can shorten it.
var pingTimeout = 15 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

// run parses CLI args and executes the requested subcommand.
// Returns 0 on success, 1 on runtime error, 2 on usage/flag error.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}
	switch args[0] {
	case "dlq":
		return runDLQ(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "-help", "help":
		printUsage(stderr)
		return 2
	default:
		fmt.Fprintf(stderr, "unknown command: %s\n", args[0])
		printUsage(stderr)
		return 2
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: offsetkeeper <command> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Commands:")
	fmt.Fprintln(w, "  dlq list    List records in a dead-letter queue")
	fmt.Fprintln(w, "  dlq replay  Replay records from a dead-letter queue")
}

func printDLQUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: offsetkeeper dlq <subcommand> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Subcommands:")
	fmt.Fprintln(w, "  list    List records in a dead-letter queue")
	fmt.Fprintln(w, "  replay  Replay records from a dead-letter queue")
}

func initFlagSet(name, usage string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: %s\n\nFlags:\n", usage)
		fs.PrintDefaults()
	}
	return fs
}

func parseBrokers(raw string) []string {
	var list []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			list = append(list, b)
		}
	}
	return list
}

// newClient creates a client for brokers, over TLS with the system root CAs when
// useTLS is set. It fails fast by pinging the brokers with a 15s timeout.
// There is no SASL; the CLI is for clusters that need nothing else.
func newClient(ctx context.Context, brokers []string, useTLS bool) (*kgo.Client, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	if useTLS {
		opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := cl.Ping(pctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("connect to brokers %s: %w (check -tls: omit if plaintext, pass if TLS required)", strings.Join(brokers, ","), err)
	}
	return cl, nil
}

// escape makes free text safe for one tab-separated line: backslash, quote,
// tab, newline and other control or non-printable characters become Go escapes.
func escape(s string) string {
	q := strconv.Quote(s)
	return q[1 : len(q)-1]
}

func validateCommon(fs *flag.FlagSet, brokers, topic string, stderr io.Writer) ([]string, bool) {
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "unexpected positional arguments: %v\n", fs.Args())
		fs.Usage()
		return nil, false
	}
	if brokers == "" || topic == "" {
		fmt.Fprintln(stderr, "error: -brokers and -topic are required")
		fs.Usage()
		return nil, false
	}
	brokerList := parseBrokers(brokers)
	if len(brokerList) == 0 {
		fmt.Fprintln(stderr, "error: -brokers must contain at least one broker address")
		fs.Usage()
		return nil, false
	}
	return brokerList, true
}

func runDLQ(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printDLQUsage(stderr)
		return 2
	}
	switch args[0] {
	case "list":
		return runDLQList(ctx, args[1:], stdout, stderr)
	case "replay":
		return runDLQReplay(ctx, args[1:], stdout, stderr)
	case "-h", "--help", "-help", "help":
		printDLQUsage(stderr)
		return 2
	default:
		fmt.Fprintf(stderr, "unknown dlq subcommand: %s\n", args[0])
		printDLQUsage(stderr)
		return 2
	}
}

func runDLQList(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := initFlagSet("dlq list", "offsetkeeper dlq list -brokers <brokers> -topic <topic> [-tls]\n\n"+
		"Rows print as they arrive: offsets in order within a partition, partitions interleaved.\n"+
		"source_topic and error are escaped (\\t, \\n, \\\\, \\\", non-printable bytes) so each record is one line.", stderr)
	brokers := fs.String("brokers", "", "comma-separated Kafka broker addresses (required)")
	topic := fs.String("topic", "", "DLQ topic name (required)")
	useTLS := fs.Bool("tls", false, "connect over TLS using the system root CAs (no SASL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	brokerList, ok := validateCommon(fs, *brokers, *topic, stderr)
	if !ok {
		return 2
	}

	cl, err := newClient(ctx, brokerList, *useTLS)
	if err != nil {
		fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
		return 1
	}
	defer cl.Close()

	// The header waits for the first row or a clean finish, so a failed List
	// (missing topic) prints no header. A failed write stops the listing.
	headerDone := false
	header := func() error {
		if headerDone {
			return nil
		}
		headerDone = true
		_, err := fmt.Fprintln(stdout, "partition\toffset\tsource_topic\tsource_partition\tsource_offset\tattempts\terror")
		return err
	}
	err = dlq.List(ctx, cl, *topic, func(e dlq.Entry) error {
		if err := header(); err != nil {
			return err
		}
		_, err := fmt.Fprintf(stdout, "%d\t%d\t%s\t%d\t%d\t%d\t%s\n", e.Record.Partition, e.Record.Offset, escape(e.SourceTopic), e.SourcePartition, e.SourceOffset, e.Attempts, escape(e.Err))
		return err
	})
	if err == nil {
		err = header()
	}
	if err != nil {
		fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
		return 1
	}
	return 0
}

func runDLQReplay(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := initFlagSet("dlq replay", "offsetkeeper dlq replay -brokers <brokers> -topic <topic> [-partition <partition> -offset <offset>] [-tls]", stderr)
	brokers := fs.String("brokers", "", "comma-separated Kafka broker addresses (required)")
	topic := fs.String("topic", "", "DLQ topic name (required)")
	useTLS := fs.Bool("tls", false, "connect over TLS using the system root CAs (no SASL)")
	partition := fs.Int("partition", -1, "partition of the DLQ record to replay (must be given with -offset)")
	offset := fs.Int64("offset", -1, "offset of the DLQ record to replay (must be given with -partition)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	brokerList, ok := validateCommon(fs, *brokers, *topic, stderr)
	if !ok {
		return 2
	}

	var partitionSet, offsetSet bool
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "partition" {
			partitionSet = true
		}
		if f.Name == "offset" {
			offsetSet = true
		}
	})
	if partitionSet != offsetSet {
		fmt.Fprintln(stderr, "error: -partition and -offset must be given together")
		fs.Usage()
		return 2
	}
	if partitionSet && (*partition < 0 || *offset < 0) {
		fmt.Fprintln(stderr, "error: -partition and -offset must be non-negative")
		fs.Usage()
		return 2
	}
	// A Kafka partition is an int32; int32(*partition) would silently wrap.
	if partitionSet && *partition > math.MaxInt32 {
		fmt.Fprintf(stderr, "error: -partition must be <= %d\n", math.MaxInt32)
		fs.Usage()
		return 2
	}

	cl, err := newClient(ctx, brokerList, *useTLS)
	if err != nil {
		fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
		return 1
	}
	defer cl.Close()

	if partitionSet {
		if err := dlq.ReplayOne(ctx, cl, *topic, int32(*partition), *offset); err != nil {
			fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
			return 1
		}
		return printReplayed(stdout, stderr, 1)
	}

	n, err := dlq.ReplayAll(ctx, cl, *topic)
	if err != nil {
		fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
		return 1
	}
	return printReplayed(stdout, stderr, n)
}

func printReplayed(stdout, stderr io.Writer, n int) int {
	if _, err := fmt.Fprintf(stdout, "replayed %d record(s)\n", n); err != nil {
		fmt.Fprintf(stderr, "offsetkeeper: %v\n", err)
		return 1
	}
	return 0
}
