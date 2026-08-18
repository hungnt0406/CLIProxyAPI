# Decision: Local Antigravity Replay Cache Storage And Operational Contract

- **Date:** 2026-08-18
- **Status:** Decided during Task 4 of the local replay cache plan
  (`docs/superpowers/plans/2026-08-18-antigravity-local-replay-cache.md`)

## Decision

Standalone replay state is stored on local disk under the *resolved*
authentication directory at `<AuthDir>/antigravity-replay`, and that storage
contract is documented as an operational note in `README.md` (no deployment
documentation file exists in the repository, and no documentation-test
framework is introduced). The documented contract:

1. **Contents** — only normalized replay items (thought signatures and
   function-call parts) plus timestamp/revision/branch/deleted fencing
   fields. Never original requests, responses, credentials, or transcripts.
2. **Permissions** — directory `0700`, files `0600` (owner-only); files with
   group/other access bits are rejected as cache misses.
3. **Retention** — entries are considered expired one hour after their last
   write; expired entries are dropped from memory and removed from disk by
   periodic cleanup (10-minute interval) and on access.
4. **Clearing and restart recovery** — operators stop CLIProxyAPI and run
   `rm -rf <AuthDir>/antigravity-replay`; after a restart, valid entries are
   hydrated from disk on the first read miss. The directory holds no
   conversation history, so removal is always safe.
5. **Home-mode authority** — when the Home KV client is active it remains
   authoritative (reads, writes, deletes, and fencing all go through Home KV);
   Home mode resets the disk root to empty so local persistence is never used.
6. **Runtime `auth-dir` behavior** — the root is resolved at startup
   (tilde-expanded, defaulted, absolutized) and re-pointed only when an
   accepted runtime configuration update changes `auth-dir`; files under a
   previous root are not migrated and may be removed manually.
7. **Fail-closed** — missing, expired, corrupt, oversized, or unsafe replay
   files are cache misses; unknown provenance still fails closed with the
   existing `missing Claude tool provenance` error.

## Context

Task 4 requires operator documentation for the persistence shipped by Tasks
1-3 plus full-suite verification. The repository has no deployment/operations
documentation file: `docs/` contains only SDK guides (`sdk-*.md`), and
`README.md` is the only operator-facing Markdown document. The repository also
has no documentation-test pattern (no golden-file or README assertions), and
the plan forbids introducing a documentation-test framework.

## Options considered

- **Documentation location**
  - **README.md (chosen)** — the established operator-facing document; the
    plan explicitly names it as the fallback target.
  - **A new deployment documentation file** — rejected: the plan says to use
    the existing operator-facing file and not to add a documentation
    framework; `docs/` is SDK-focused.
  - **`config.example.yaml` comments** — rejected: a YAML reference is not
    operator documentation and `auth-dir` is already described there.
- **Documentation tests**
  - **No tests (chosen)** — the repository has no documentation-test pattern;
    the brief's Step 1 is conditional on one existing, and the required
    verification is a documented command sequence plus `go test ./...`.
  - **Adding a doc-lint/golden-file test** — rejected: explicitly forbidden by
    the plan and not the repo's convention.
- **Retention phrasing** — "one hour after the last write" (chosen) rather
  than "last access": disk files are rewritten only on mutations, reads
  refresh the in-memory timestamp but never the persisted file, so the
  persisted TTL is bounded by the last write.

## Reasoning

- Every documented claim was verified against the implementation before
  writing: `os.MkdirAll(dir, 0o700)` + `os.CreateTemp` (0600) in
  `antigravity_reasoning_replay_disk.go`; the `AntigravityReasoningReplayCacheTTL`
  one-hour constant; `now.Sub(fileEntry.Timestamp) > TTL` rejection and
  best-effort removal on load; `purgeExpiredAntigravityReasoningReplayCache`
  deleting disk files for expired keys; Home-KV-first branching in every
  public cache function; and the end-of-`applyConfigRuntime` rewire added in
  the Task 3 review rounds.
- The `rm -rf` clearing instruction matches the established
  `ClearAntigravityReasoningReplayCache` semantics and the spec's
  "Operational Impact" section; the decision record from Task 2 already
  committed to documenting exactly this command.
- Documenting "fail-closed" matters operationally: it reassures operators
  that deleting or corrupting these files degrades to the pre-existing
  error, never to accepting unknown provenance.

## Impact

- Operators get a single authoritative place (`README.md`) describing where
  replay state lives, what it contains, how it is protected, when it
  expires, how to clear it, and what it cannot do.
- No test behavior changes: no documentation-test framework was introduced,
  and the full Go suite remains the verification gate.
- The decision record itself lives under `docs/superpowers/decisions/`, which
  `.gitignore` covers (`docs/*`); it is committed with `git add -f`, the same
  mechanism the earlier spec/decision files used.

## Next steps

- Run `gofmt -l`/`gofmt -w` on the tree (no Go files change in this task),
  run `go test ./...`, self-review both documents against the implementation,
  and commit:
  `git add README.md && git add -f docs/superpowers/decisions/2026-08-18-local-replay-cache-storage.md && git commit -m "docs: document local antigravity replay persistence"`.
