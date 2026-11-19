## What and why
<!-- Why this change is needed; link the issue: Fixes #n -->

## Checklist
- [ ] PR title is a Conventional Commit: `type(scope): subject` (squash-merge uses it)
- [ ] Issue linked for features; reason given for any new exported identifier
- [ ] Contract/test changed first, and the new test failed before the code change
- [ ] `gofmt -l .`, `go vet ./...`, `golangci-lint run`, `go test -race -count=1 ./...` pass
- [ ] Real brokers run (kafka, redpanda), or "real-broker untested" stated here
- [ ] `CHANGELOG.md` updated under *Unreleased* (user-visible changes)
- [ ] `apidiff`/`gorelease` output pasted if exported API changed
- [ ] Doc comments and `CONTRACT.md` updated
- [ ] No new dependency beyond stdlib and franz-go

## Testing
<!-- Commands run and results; mutation check you did (what you broke, which test failed) -->
