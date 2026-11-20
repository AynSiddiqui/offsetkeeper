# Contributing

offsetkeeper is small on purpose. Agents implement against the tests, and the
owner can explain every line of the commit path. Keep it that way.

Code rules are in [STYLE.md](STYLE.md). Report vulnerabilities as described in
[SECURITY.md](SECURITY.md), never in a public issue. Everyone follows the
[Code of Conduct](CODE_OF_CONDUCT.md).

## Workflow
1. **Issue first for features.** Open an issue and get a maintainer's go-ahead
   before writing code. Bug fixes, docs and typos can go straight to a PR.
   A new exported identifier needs a stated reason in the issue and the PR.
2. **Branch** from `main` named `type/short-desc`, with `type` from the commit
   types below: `feat/dlq-headers`, `fix/commit-after-rebalance`, `docs/readme-quickstart`.
3. **Tests first.** [CONTRACT.md](CONTRACT.md) is the spec. Change the contract
   and its named test first, see the test fail, then write the code.
4. **Open a PR** against `main`. The PR template is the checklist; every box is
   ticked or marked n/a with a reason.
5. **Squash-merge.** The PR title becomes the commit subject, so it must be a
   valid Conventional Commit (CI checks it). The PR description becomes the body.

### PR checklist
- [ ] Issue linked (`Fixes #n`) for features; reason given for any new exported identifier
- [ ] Contract/test changed first; new behavior has a test that failed before the change
- [ ] `gofmt -l .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...` pass
- [ ] Real brokers run if consume, commit, retry or DLQ paths changed, else "real-broker untested" stated
- [ ] `CHANGELOG.md` line under *Unreleased* for any user-visible change
- [ ] `apidiff` output pasted if exported API changed
- [ ] Doc comments and `CONTRACT.md` updated; no new dependency beyond stdlib and franz-go

## Commits
Format: [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).

```
type(scope): subject

body: why the change is needed and what it trades off.

BREAKING CHANGE: what breaks and how to migrate.   (only when it does)
```

- **Types:** `feat` `fix` `docs` `test` `refactor` `perf` `build` `ci` `chore` `revert`.
- **Scope** is the Go package path's last element: `cmd` (for `cmd/offsetkeeper`), `dlq`, `testenv`, or `offsetkeeper` (the library package); `deps` is reserved for Dependabot.
- **Breaking:** add `!` before the colon (`feat(dlq)!: ...`) and a `BREAKING CHANGE:` footer.
- Header: imperative, lowercase after the colon, no trailing period, ≤72 characters total ("fix(dlq): wait for ack before committing").
- Body: wrap at 72, explain *why*, not what the diff shows. Include `benchstat`
  output for `perf`. Reference issues with `Fixes #n` or `Updates #n`.
- Every commit that contains Go packages builds and passes tests, so `git bisect` always works.
- No AI attribution lines: no `Co-Authored-By`, no "Generated with".
- No sign-off (DCO) needed: inbound contributions are under the Apache-2.0
  license, section 5.

## Review
- One logical change per PR. Small PRs get reviewed; large ones get split.
- Treat each review comment as a ticket: change the code or reply with why not.
- Reviewers check, in this order: the contract still holds, tests prove it,
  ownership of every goroutine, every wait bounded, API surface, style.
- Commit-path code (consume, retry, DLQ write, offset commit) needs the
  maintainer's approval (CODEOWNERS) and a reviewer able to explain each line.
- Reviews cover the implementation, not whether the feature should exist; that
  was settled in the issue.
- Use `fixup` commits while a review is open; the squash merge removes them.

## Code
Full rules and sources: [STYLE.md](STYLE.md). The short list:
- Every goroutine has an owner that waits for it (`sync.WaitGroup`,
  `errgroup`-style). No fire-and-forget.
- Every blocking call takes a `context.Context`.
- No `time.Sleep` in library or tests. Use timers with `ctx` in the library
  and a poll-until-deadline helper in tests.
- Wrap errors with `%w` and include topic, partition and offset where known.
- Log with `log/slog` only.
- Every exported identifier has a doc comment.
- Comments explain why, not what.
- Run `gofmt`, `go vet` and `golangci-lint run` before pushing.
- Keep the exported API minimal.

## Repository layout

- `offsetkeeper/`: the library package (import path `github.com/AynSiddiqui/offsetkeeper/offsetkeeper`) and its tests. In-package tests sit beside the code to reach unexported symbols and the `export_test.go` hooks; `contract_test.go` and `example_test.go` are black-box (`offsetkeeper_test`). The root holds no Go files.
- `test/edge/`: black-box edge suites (config, lifecycle, records) that need no hooks.
- `internal/harness/`: helpers shared by `offsetkeeper/contract_test.go` and `test/edge/` (`NewEnv`, `Start`, `Calls`, ...). Import it only from external test packages: it imports `offsetkeeper`, so an in-package test would create a cycle.
- `internal/testenv/`: broker, topic and produce helpers; safe for in-package tests.
- `internal/dlq/`: the dead-letter record format and replay.
- `cmd/offsetkeeper/`: the CLI.
- `docs/`: contributor guidelines and the contract.

## Tests
- Always `-race`: `go test -race -count=1 ./...`.
- Default runs use an in-process kfake cluster, no Docker needed.
- Real brokers: `docker compose up -d kafka` (or `redpanda`), then
  `OFFSETKEEPER_BROKERS=localhost:9092 go test -race -count=1 ./...`
  (Redpanda: `localhost:19092`). Auto topic creation is off; tests create
  topics via kadm.
- Each test uses a unique topic and group (`internal/testenv`) so tests never
  share state.
- Say "real-broker untested" in the PR if you did not run the broker suites.
- **Contract-test-first:** every guarantee in `CONTRACT.md` names the test that
  proves it. A guarantee without a test is not a guarantee.
- **Mutation mindset:** before you finish, break the code on purpose (commit
  one offset too far, skip the DLQ ack wait, drop a `wg.Wait`) and confirm a
  test fails. If none does, the test is too weak; fix the test.
- Failure messages: `Run(...) = got, want want`, with the input.
- Bug fixes ship with a test that fails without the fix.
- Benchmarks for performance claims; record settings (see STYLE.md).

## Versioning and release
- [SemVer](https://semver.org/). Before v1.0.0 the API may change in a minor
  release; say so in the changelog.
- [Keep a Changelog](https://keepachangelog.com/en/1.1.0/): add a line under
  *Unreleased* in the PR, using *Added*, *Changed*, *Deprecated*, *Removed*,
  *Fixed* or *Security*. At release, move the lines under `## vX.Y.Z - YYYY-MM-DD`.
- Compatibility follows [Keeping your modules compatible](https://go.dev/blog/module-compatibility):
  add, do not change or remove. Deprecate with a `Deprecated:` comment for at
  least one minor release before removal in a major.
- **Before any release that touches exported API**, run an API diff against the
  last tag and paste the output in the release PR:
  ```bash
  go install golang.org/x/exp/cmd/apidiff@latest
  go install golang.org/x/exp/cmd/gorelease@latest
  gorelease -base="$PREV_TAG"     # suggests the version and flags incompatible changes
  ```
  For the first release tag (`v0.1.0`), run `gorelease -base=none -version=v0.1.0`.
  An incompatible change needs a major bump (or `0.y` minor while pre-1.0)
  and a `BREAKING CHANGE:` footer.
- Release steps: green CI on `main`, stage dated `## v0.1.0 - YYYY-MM-DD` changelog section and README status, update `CHANGELOG.md`, tag `vX.Y.Z`
  (annotated), push the tag, create the GitHub release from the changelog
  section. Releases are not signed and ship no SBOM.
- **Post-tag smoke test** in a fresh module without `replace`:
  ```bash
  SMOKE_DIR=$(mktemp -d)
  cd "$SMOKE_DIR"
  go mod init example.com/smoke
  go get github.com/AynSiddiqui/offsetkeeper/offsetkeeper@vX.Y.Z
  awk '/^```go/{flag=1; next} /^```/{if(flag) exit} flag{print}' "$REPO_ROOT/README.md" > main.go
  go build .
  go install github.com/AynSiddiqui/offsetkeeper/cmd/offsetkeeper@v0.1.0
  rm -rf "$SMOKE_DIR"
  ```
- **pkg.go.dev check**: verify documentation and example rendering at `https://pkg.go.dev/github.com/AynSiddiqui/offsetkeeper/offsetkeeper@vX.Y.Z`.
- **GitHub settings checklist**:
  - Private vulnerability reporting enabled (Settings → Code security and analysis).
  - Squash merging only (disallow merge commits and rebase merging).
  - Required status checks after the first green run on `main`.
  - Code-owner review configured without blocking the solo maintainer (allow maintainer bypass or omit mandatory 2-person approval).

## Dependencies
Stdlib and franz-go only. No testify, no testcontainers. Dependabot opens
weekly update PRs (`build(deps)` for Go modules, `ci(deps)` for Actions).
A new dependency needs an issue and a reason the stdlib cannot do the job.
