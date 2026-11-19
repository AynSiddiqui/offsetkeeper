# Style

Engineering rules for offsetkeeper. Each section names its source; where this
file is stricter than the source, this file wins. Tooling enforces what it can
(`gofmt`, `go vet`, `golangci-lint`); the rest is checked in review.

## Layout
- Flat root package `offsetkeeper` for the public API; support code in `internal/`;
  binaries in `cmd/<name>/`. No `pkg/` directory.
- Keep tightly coupled types in one package. Never name a package `util`, `common` or `helper`.
- Files are focused: neither one giant file nor many tiny ones.

Source: [Organizing a Go module](https://go.dev/doc/modules/layout), [Google best practices: package size](https://google.github.io/styleguide/go/best-practices.html#package-size).

## Naming
- `MixedCaps`, no underscores (test names may use them). Initialisms keep case: `ID`, `URL`, `topicID`.
- Short receiver names, the same on every method. No `this`/`self`. No `Get` prefix on getters.
- Do not repeat the package name: `offsetkeeper.Run`, not `offsetkeeper.RunConsumer`.
- Short names in small scopes, descriptive names in wide scopes.
- Interfaces are defined by the consumer, return concrete types, and do not exist before a second implementation does.

Source: [Code Review Comments](https://go.dev/wiki/CodeReviewComments), [Google decisions: naming](https://google.github.io/styleguide/go/decisions.html#naming).

## Errors
- Return errors; never `panic` for control flow (a recovered handler panic becomes an error).
- Strings are lowercase, no trailing punctuation, and start with `offsetkeeper:` at the API edge.
- Wrap with `%w` when callers may match the cause; include topic, partition and offset where known. No "failed to".
- Handle an error once: return it or log it, not both.
- Expose sentinels or types for `errors.Is`/`errors.As`; never make callers match strings.
- Never discard an error with `_` unless a comment says why.

Source: [Google best practices: error handling](https://google.github.io/styleguide/go/best-practices.html#error-handling), [Uber: error wrapping](https://github.com/uber-go/guide/blob/master/style.md#error-wrapping), [Effective Go](https://go.dev/doc/effective_go#errors) (frozen since 2009, so only the basics).

## Context
- `ctx context.Context` is the first parameter of every function that blocks or does I/O.
- Never store a context in a struct, never define a custom context type, never put `ctx` in an options struct.
- `context.Background()` appears only in `main` and in cleanup that must outlive cancellation (use `context.WithoutCancel` plus a timeout).
- Tests derive `ctx` with `context.WithTimeout`, never an unbounded one.

Source: [Code Review Comments: contexts](https://go.dev/wiki/CodeReviewComments#contexts), [Google decisions: contexts](https://google.github.io/styleguide/go/decisions.html#contexts).

## Concurrency and goroutine ownership
- Prefer synchronous functions; the caller adds concurrency.
- Every `go` statement has one owner that waits for it (`sync.WaitGroup` or `errgroup`) and a documented stop condition (ctx cancellation or channel close).
- No goroutines in `init` or package-level vars.
- Data crosses goroutines by channel or by a result slot written by one goroutine and read after `Wait` (see `partResult`). A mutex is fine for a small guarded value; document what it guards.
- Document concurrency safety on every exported type: safe for concurrent use or not.
- A goroutine that can panic must have its failure turned into an error for the owner.

Source: [Uber: don't fire-and-forget goroutines](https://github.com/uber-go/guide/blob/master/style.md#dont-fire-and-forget-goroutines), [Google decisions: goroutine lifetimes](https://google.github.io/styleguide/go/decisions.html#goroutine-lifetimes), [Google best practices: documenting concurrency](https://google.github.io/styleguide/go/best-practices.html#documenting-concurrency).

## Logging
- `log/slog` only, through the `Config.Logger` (nil means `slog.Default()`). No `log`, no `fmt.Print*` in the library.
- Structured attributes (`"topic"`, `"partition"`, `"offset"`, `"attempt"`, `"err"`), never string-built messages.
- Log at the point you handle an error, not where you return it. No secrets or record values in logs.

Source: [log/slog](https://pkg.go.dev/log/slog), [Google best practices: error handling (log sparingly)](https://google.github.io/styleguide/go/best-practices.html#error-handling).

## Config validation
- Validate at the boundary (`Config.withDefaults`): reject bad input with an error that names the field and the bad value; never fix it silently.
- The zero value of every optional field means "default", and the default is in the field's doc comment.
- Optional settings grow as struct fields (new fields must keep the old behavior at zero); never add a positional parameter to a released function.
- Do not mutate the caller's config.

Source: [Google best practices: option structure](https://google.github.io/styleguide/go/best-practices.html#option-structure), [Keeping your modules compatible](https://go.dev/blog/module-compatibility).

## Globals
- No package-level mutable state, no `init()` side effects, no flags registered by the library (only `package main` defines flags).
- The only allowed globals are constants, immutable values, and test hooks in `export_test.go`.
- Inject clocks and clients through struct fields.

Source: [Google decisions: global state](https://google.github.io/styleguide/go/decisions.html#global-state), [Uber: avoid mutable globals](https://github.com/uber-go/guide/blob/master/style.md#avoid-mutable-globals), [Uber: avoid init()](https://github.com/uber-go/guide/blob/master/style.md#avoid-init).

## Docs
- Every package has a `Package x ...` comment (one file); every exported identifier has a doc comment that starts with its name and is a full sentence. Use "reports whether" for bool results.
- Comments say why, not what. Mark removals with a `Deprecated:` paragraph that names the replacement.
- Use doc links (`[Run]`, `[kgo.Record]`), `# Heading`s sparingly, and indented code for snippets.
- Usage lives in testable `Example` functions in `example_test.go` (package `offsetkeeper_test`). Examples that need a broker compile but have no `// Output:`.
- A guarantee change updates `CONTRACT.md` and its named test in the same PR.

Source: [Go Doc Comments](https://go.dev/doc/comment), [Google best practices: examples](https://google.github.io/styleguide/go/best-practices.html#examples).

## Tests
- Table tests where cases differ only by data; one `Test` per contract guarantee, named in `CONTRACT.md`.
- Assertions stay in the `Test` function. Helpers call `t.Helper()`, do setup and cleanup (`t.Cleanup`), and use `t.Fatal` only for setup.
- Failure messages: `F(input) = got, want want`. Use `t.Error` to keep checking; `t.Fatal` only when later checks are meaningless. `cmp.Diff`-style output, no assertion libraries.
- Unique topic and group per test (`internal/testenv`). No `time.Sleep`: poll to a deadline.
- Run with `-race -count=1`. Default is kfake; real-broker suites run in CI and on request (`OFFSETKEEPER_BROKERS`).
- Peer projects ask the same: client_golang and OpenTelemetry-Go require tests with each change and benchmarks (with `benchstat`) for performance work; franz-go ships kfake so features are tested in-process; Watermill ships a pub/sub conformance suite, which is what `contract_test.go` does here.

Source: [Google decisions: test failure messages](https://google.github.io/styleguide/go/decisions.html#useful-test-failures), [Google best practices: test helpers](https://google.github.io/styleguide/go/best-practices.html#test-helpers), [OpenTelemetry-Go CONTRIBUTING: tests](https://github.com/open-telemetry/opentelemetry-go/blob/main/CONTRIBUTING.md).

## Dependencies
- Stdlib and franz-go only (`kgo`, `kadm`, `kfake`). No testify, no testcontainers, no logging or metrics frameworks.
- A new dependency needs an issue first: what the stdlib cannot do, its maintenance state, its transitive cost.
- Keep `go.mod` tidy; update with `go get` then `go mod tidy`; never hand-edit `// indirect`. No `vendor/`.
- `govulncheck` runs in CI and must pass.

Source: [Prometheus client_golang: managing dependencies](https://github.com/prometheus/client_golang/blob/main/CONTRIBUTING.md#managing-dependencies), [Go modules reference](https://go.dev/ref/mod).

## Performance
- Measure first. No optimization without a benchmark that shows the cost and one that shows the gain.
- Benchmarks use `testing.B`, `b.ReportAllocs()`, and run with `-count=10`; compare with `benchstat`.
- Record the settings next to every published number: Go version, OS and CPU, broker (kfake, Kafka version or Redpanda version), partitions, record size, batch size, `MaxAttempts`, and `GOMAXPROCS`. A number without settings is not a result.
- Correctness beats speed: a faster path that weakens a contract guarantee is rejected.
- Cheap by default: preallocate slices with known size, avoid `fmt` on hot paths, but only change code a benchmark points at.

Source: [Uber: performance](https://github.com/uber-go/guide/blob/master/style.md#performance), [Go Contribute: benchstat in commit messages](https://go.dev/doc/contribute#commit_messages), [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).

## Reliability rules for this library
These come from CONTRACT.md and are never traded for convenience.
1. **Never commit past unsettled.** An offset is committed only for a contiguous prefix of settled records (handled without error, or DLQ-acked) per partition. A failed, skipped or in-flight record blocks its partition's commit.
2. **DLQ write is acked before the source commit.** If the DLQ cannot be written, `Run` stops with an error; it never drops the record.
3. **Every wait has a `ctx` or a timeout.** Network calls, commits, DLQ produce, retries and shutdown. Commits ignore the run `ctx` on shutdown, so they carry their own timeout.
4. **Handlers may see a record twice.** Document it; never promise exactly-once.
5. **A handler panic or `runtime.Goexit` is a failed attempt**, not a crash and not a success.
6. **Retries and backoff are bounded** (`MaxAttempts`, capped backoff) and must fit the group rebalance timeout; document the budget.
7. **Shutdown order:** stop polling, wait for goroutines, commit settled offsets, close the client.
8. **Weakening options are documented, not hidden.** If a user can void a guarantee through `ClientOpts`, the doc comment says which options.

Source: this repository's [CONTRACT.md](CONTRACT.md); [Uber: no fire-and-forget](https://github.com/uber-go/guide/blob/master/style.md#dont-fire-and-forget-goroutines).
