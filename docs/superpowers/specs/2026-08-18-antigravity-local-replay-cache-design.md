# Antigravity Local Replay Cache

## Decision

Add a local disk-backed persistence layer for standalone Antigravity reasoning replay state. Keep the existing in-memory cache as the fast path, persist normalized replay entries under the CLIProxyAPI authentication directory, and preserve fail-closed provenance validation when state is unavailable or invalid.

## Problem

Standalone CLIProxyAPI currently stores Antigravity replay provenance in process memory. OpenCode can resume a conversation after CLIProxyAPI restarts, but the proxy then lacks the native tool IDs and thought signatures needed to translate the resumed Claude-compatible tool history. The request is rejected with `missing Claude tool provenance`.

## Goals

- Restore valid replay state after a CLIProxyAPI process restart.
- Preserve session and model isolation.
- Reuse the existing replay cache TTL, entry limits, normalization, and invalidation behavior.
- Keep the cache private to the local user.
- Avoid persisting full prompts, transcripts, credentials, or unrelated request data.
- Keep unknown or corrupted provenance fail-closed.
- Avoid adding a new external service or dependency for standalone mode.

## Non-goals

- Reconstructing provenance that was never captured.
- Extending the one-hour replay lifetime.
- Replacing Home-mode KV persistence.
- Making replay state portable between machines.
- Weakening signature or function-call pairing validation.

## Design

The existing Antigravity replay cache remains the public integration point. Its in-memory map continues to serve reads and writes during normal operation. A disk store is initialized lazily and uses the existing cache directory root, with a dedicated `antigravity-replay` subdirectory.

Each model/session cache key maps to one encoded entry containing the already-normalized replay items and the metadata required by the current cache implementation for TTL, revision, branch, and deletion fencing. The persisted representation must not include the original request or response transcript.

Writes use a temporary file in the target directory, flush its contents, set owner-only permissions, and atomically rename it into place. Reads reject malformed JSON, unexpected fields or sizes, expired entries, and files with unsafe permissions. Corrupt or expired files are removed best-effort and treated as cache misses.

The process loads entries on first access rather than scanning the entire cache during server startup. Loading a disk entry populates the in-memory map, after which existing memory eviction and TTL logic applies. Deletion removes both memory and disk state. Home mode remains authoritative when its KV client is active and does not use the local disk store.

## Data Safety

- Cache directory permissions: `0700`.
- Cache file permissions: `0600`.
- File names are derived from a safe digest of model and session key, never from raw user-controlled path text.
- Temporary files are created inside the cache directory so rename remains atomic on the same filesystem.
- Writes are bounded by the existing per-entry item and byte limits before serialization.
- Cache failures are non-fatal for writes but must not cause untrusted provenance to be accepted.

## Testing

Tests will cover:

- A normalized entry can be written, loaded by a fresh cache instance, and returned on the next request.
- Entries from different models or sessions do not cross-match.
- Expired entries are ignored and cleaned up.
- Corrupt and oversized files are ignored without panicking.
- Atomic replacement leaves the previous valid entry available when a write fails.
- Deletion removes persisted state.
- Home-mode KV behavior remains unchanged.
- Existing fail-closed missing-provenance and pairing tests continue to pass.

## Operational Impact

Standalone deployments gain replay continuity across proxy restarts without additional infrastructure. The cache contains sensitive tool arguments and thought signatures, so it must be stored below the existing private authentication directory and excluded from version control. The one-hour TTL bounds disk retention; operators can remove the directory to clear replay state manually.
