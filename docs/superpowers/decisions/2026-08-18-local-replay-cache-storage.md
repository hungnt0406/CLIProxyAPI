# Decision: Local Antigravity Replay Cache Storage And Operational Contract

- **Date:** 2026-08-18
- **Status:** Decided during Task 4 of the local replay cache plan
  (`docs/superpowers/plans/2026-08-18-antigravity-local-replay-cache.md`).
  Revised after review round 1 (same day): retention stated per layer
  (memory refresh vs. file write timestamp), the contents claim scoped to
  normalized function-call arguments, and cleanup wording made
  eviction-aware.

## Decision

Standalone replay state is stored on local disk under the *resolved*
authentication directory at `<AuthDir>/antigravity-replay`, and that storage
contract is documented as an operational note in `README.md` (no deployment
documentation file exists in the repository, and no documentation-test
framework is introduced). The documented contract:

1. **Contents** — only normalized replay items (thought signatures and
   function-call parts) plus timestamp/revision/branch/deleted fencing
   fields. Original requests, responses, auth files, and conversation
   transcripts are never serialized; normalized function-call arguments
   may contain application-supplied sensitive data.
2. **Permissions** — directory `0700`, files `0600` (owner-only); files with
   group/other access bits are rejected as cache misses.
3. **Retention** — in-memory entries expire one hour after their last
   access and are refreshed on every read, so an actively used conversation
   stays valid while the process runs. Persisted files keep the timestamp
   of their last write and are rejected (and removed best-effort) when
   loaded more than one hour old. Periodic cleanup (10-minute interval)
   expires resident entries and removes their files; files for
   memory-evicted entries may remain until a later load rejects and removes
   them, or the directory is cleared manually.
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
- **Retention phrasing** — the contract states each layer's lifetime
  separately, after review: memory entries expire one hour after their last
  access refresh (reads touch memory only), while persisted files keep
  their write timestamp and are rejected when loaded more than one hour
  old. A strict "one hour after the last write" for the combined entry
  would be wrong — an actively used conversation is refreshed in memory —
  and "one hour after last access" would be wrong for the file, which reads
  never rewrite.

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
- Retention is stated per layer because the implementation enforces
  different timestamps for memory and disk: reads refresh the in-memory
  timestamp (`entry.Timestamp = now` on hit and on hydration) but never
  rewrite the file, whose timestamp is fixed at save time; cleanup is
  best-effort for evicted keys, so the docs say only what the code
  guarantees.
- The contents claim is deliberately scoped: the disk file shape carries
  only normalized items and fencing fields, but `function_call_part` items
  store `name` and `args` verbatim, so application-supplied tool arguments
  can be sensitive; "never serialized" applies to original requests,
  responses, auth files, and transcripts only.
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

- Review round 1 corrections committed as `docs: correct replay cache
  persistence wording` on top of the initial `docs: document local
  antigravity replay persistence` commit; verification is `gofmt -l` and
  `go test ./...`.
