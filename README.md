# offsetkeeper

A handler function with retries and a dead-letter queue in 20 lines, and it
cannot commit past a failed record. A Kafka consumer runtime for Go on franz-go.

**Status:** pre-release, not yet tagged.

- [CONTRACT.md](CONTRACT.md): what is guaranteed, and the test behind each guarantee
- [CONTRIBUTING.md](CONTRIBUTING.md): how the code is written

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for workflow and commits, [STYLE.md](STYLE.md) for code rules, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## Quickstart

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/AynSiddiqui/offsetkeeper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := offsetkeeper.Run(ctx, offsetkeeper.Config{
		Brokers:     []string{"localhost:9092"},
		Group:       "my-group",
		Topics:      []string{"my-topic"},
		MaxAttempts: 3,
		DLQTopic:    "my-topic.dlq",
		Handler: func(_ context.Context, r *kgo.Record) error {
			log.Printf("handled %s", r.Value)
			return nil
		},
	}); err != nil {
		log.Fatal(err)
	}
}
```

## What it guarantees

See [CONTRACT.md](CONTRACT.md) for the full contract and test suite.

- **At-least-once delivery.** Handlers may see a record more than once and must be idempotent.
- **Per-partition order.** Records are handled sequentially in offset order within a partition.
- **Confirmed DLQ before commit.** A record that still fails after its retries goes to the DLQ, and its source offset is committed only after the broker acks the DLQ write.
- **No commit past failure.** Offsets never advance beyond an unsettled or failed record.
- **Rebalance timeout.** Handlers must finish (retries included) within the rebalance timeout.

## DLQ tools

Install the CLI tool:

```bash
go install github.com/AynSiddiqui/offsetkeeper/cmd/offsetkeeper@latest
```

List records in a dead-letter queue:

```bash
offsetkeeper dlq list -brokers localhost:9092 -topic my-topic.dlq
```

Replay records back to their source topics:

```bash
# Replay all records not yet replayed by a previous full replay
offsetkeeper dlq replay -brokers localhost:9092 -topic my-topic.dlq

# Replay a single record by partition and offset
offsetkeeper dlq replay -brokers localhost:9092 -topic my-topic.dlq -partition 0 -offset 42
```

Running `replay` without `-partition` and `-offset` replays every record not yet replayed by a previous full replay (tracked via a consumer group). A single-record replay is not tracked.

## How this is built

Agents implement against the tests, and the owner can explain every line of the
commit path.

## License

Apache-2.0. See [LICENSE](LICENSE).
