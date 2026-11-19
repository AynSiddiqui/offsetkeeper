# offsetkeeper

A Kafka consumer runtime for Go on franz-go with retries and a dead-letter
queue that cannot commit past a failed record.

**Status:** pre-release, not yet tagged.

- [CONTRACT.md](CONTRACT.md): what is guaranteed, and the test behind each guarantee
- [CONTRIBUTING.md](CONTRIBUTING.md): how the code is written

## Prerequisites

- Go ≥1.26.
- A running Kafka cluster (or wire-compatible broker such as Redpanda).
- Source topics must be pre-created.
- The DLQ topic must be pre-created as a plain, non-compacted, non-transactional topic with `max.message.bytes` ≥ source message size plus headers (~4 KiB).

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

## Operating

- **Topics & DLQ:** Pre-create source topics. DLQs must be plain, non-compacted, non-transactional topics with room for source records plus 4 KiB headers.
- **Time budget:** Budget serial handlers, retries, backoffs, DLQ writes and commits to finish within the group rebalance timeout.
- **Replay safety:** Run only one `dlq replay` at a time per DLQ. Replay may change record partitions and can duplicate records after a crash; handlers must be idempotent.
- **CLI flags:** The CLI connects via plaintext by default, or with `-tls` using system roots; SASL is not supported.

## When not to use

- **Non-Kafka brokers:** offsetkeeper targets Kafka (and Kafka-wire-compatible brokers like Redpanda) via franz-go; it does not support Pulsar, RabbitMQ, or other messaging protocols.
- **Exactly-once processing:** Delivery is at-least-once; side effects must be idempotent.
- **Key-parallel processing:** Records are processed sequentially per partition. If you require concurrent handling of distinct keys within the same partition, offsetkeeper does not support key-parallelism yet.

## DLQ tools

Install the CLI tool:

```bash
go install github.com/AynSiddiqui/offsetkeeper/cmd/offsetkeeper@latest
```

List records in a dead-letter queue (streams rows as they are read; free-text columns are escaped):

```bash
offsetkeeper dlq list -brokers localhost:9092 -topic my-topic.dlq
```

Replay records back to their source topics (use `-tls` for TLS clusters with system roots):

```bash
# Replay all records not yet replayed by a previous full replay
offsetkeeper dlq replay -brokers localhost:9092 -topic my-topic.dlq

# Replay a single record by partition and offset
offsetkeeper dlq replay -brokers localhost:9092 -topic my-topic.dlq -partition 0 -offset 42
```

Running `replay` without `-partition` and `-offset` replays every record not yet replayed by a previous full replay (tracked via a consumer group). A single-record replay is not tracked. Run only one replay at a time per DLQ topic.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for workflow and commits, [STYLE.md](STYLE.md) for code rules, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## How this is built

Agents implement against the tests, and the owner can explain every line of the
commit path.

## License

Apache-2.0. See [LICENSE](LICENSE).
