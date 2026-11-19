# offsetkeeper

A Kafka consumer runtime for Go on franz-go with retries and a dead-letter
queue that cannot commit past an unsettled record.

**Status:** pre-release, not yet tagged.

- [CONTRACT.md](CONTRACT.md): what is guaranteed, and the test behind each guarantee
- [CONTRIBUTING.md](CONTRIBUTING.md): how the code is written

## Prerequisites

- Go ≥1.26.
- A running Kafka cluster (or wire-compatible broker such as Redpanda).
- Source topics must be pre-created.
- The DLQ topic must be pre-created as a plain, non-compacted, non-transactional topic with `max.message.bytes` and producer `ProducerBatchMaxBytes` large enough to fit source key, value and headers plus ~4.2 KiB of offsetkeeper headers.

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
- **No commit past failure.** Offsets never advance beyond an unsettled record (a record that failed without a DLQ, or whose DLQ write failed).
- **Rebalance timeout.** Handlers and retries must finish within the rebalance timeout if a rebalance begins mid-batch.

## Operating

- **Topics & sizing:** Pre-create source topics. A DLQ record equals source key+value+headers plus ~4.2 KiB of offsetkeeper headers; the DLQ topic `max.message.bytes` and producer `ProducerBatchMaxBytes` must both fit it. Records above ~1 MB need `kgo.ProducerBatchMaxBytes` in `ClientOpts` for the DLQ write (franz-go default producer batch limit is ~1 MB). If a consumed topic is deleted while running, it is logged as a fetch error and `Run` keeps running.
- **Time budget:** Budget serial handlers, retries, backoffs, DLQ writes and commits to finish within the group rebalance timeout if a rebalance begins mid-batch. On shutdown, `Run` waits for in-flight handlers to return before committing (no internal drain timeout); handlers must honour `ctx`.
- **Replay limits:** Run one `dlq replay` at a time per DLQ. Replay routes via the partitioner and strips reserved `offsetkeeper.` headers. Subject to broker retention and group offset expiry (which can cause duplicates); `ReplayOne` does not advance the bulk checkpoint. An invisible tail offset waits until cancelled (Ctrl-C).
- **CLI flags:** The CLI connects via plaintext by default, or with `-tls` using system roots; SASL is not supported.

## When not to use

- **Non-Kafka brokers:** offsetkeeper targets Kafka (and Kafka-wire-compatible brokers like Redpanda) via franz-go; it does not support Pulsar, RabbitMQ, or other messaging protocols.
- **Exactly-once processing:** Delivery is at-least-once; side effects must be idempotent.
- **Key-parallel processing:** Records are processed sequentially per partition. If you require concurrent handling of distinct keys within the same partition, offsetkeeper does not support key-parallelism yet.

## DLQ tools

Install the CLI tool:

```bash
# Before the first tag, from a checkout:
go install ./cmd/offsetkeeper

# After the v0.1.0 release tag:
go install github.com/AynSiddiqui/offsetkeeper/cmd/offsetkeeper@v0.1.0
```

### Local demo

```bash
docker compose up -d kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic.dlq --partitions 1 --replication-factor 1
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic my-topic <<< "hello offsetkeeper"
go run .
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
