package cache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	homekv "github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	// AntigravityReasoningReplayCacheTTL limits how long encrypted reasoning replay
	// items stay in process memory.
	AntigravityReasoningReplayCacheTTL = 1 * time.Hour

	// AntigravityReasoningReplayCacheMaxEntries bounds process memory for replay
	// continuity. Oldest entries are evicted first.
	AntigravityReasoningReplayCacheMaxEntries = 10240

	// AntigravityReasoningReplayCacheEvictBatchSize leaves headroom after the cache
	// reaches capacity so high write volume does not rescan the map every turn.
	AntigravityReasoningReplayCacheEvictBatchSize = 128

	minAntigravityThoughtSignatureReplayLen = 16

	// AntigravityReasoningReplayCacheMaxItemsPerEntry and MaxBytesPerEntry
	// bound one logical conversation. Oversized chains are not partially cached,
	// because dropping an arbitrary prefix would break native signature ordering.
	AntigravityReasoningReplayCacheMaxItemsPerEntry = 4096
	AntigravityReasoningReplayCacheMaxBytesPerEntry = 16 << 20

	// JSON encodes each normalized []byte item as base64. Leave enough room for
	// that expansion while rejecting oversized Home values before unmarshalling.
	antigravityReasoningReplayCacheMaxSerializedBytes = 24 << 20
)

type antigravityReasoningReplayEntry struct {
	Items     [][]byte
	Timestamp time.Time
	Revision  uint64
	Branch    string
	Deleted   bool
}

const antigravityReasoningReplayGenerationItemType = "cpa_antigravity_replay_generation"

// AntigravityReasoningReplaySnapshot identifies the exact replay state read for
// one request. Its fields are intentionally opaque outside this package.
type AntigravityReasoningReplaySnapshot struct {
	raw           []byte
	items         [][]byte
	loaded        bool
	found         bool
	revision      uint64
	branch        string
	evictionEpoch uint64
}

var (
	antigravityReasoningReplayMu            sync.Mutex
	antigravityReasoningReplayEntries       = make(map[string]antigravityReasoningReplayEntry)
	antigravityReasoningReplayNextRevision  uint64
	antigravityReasoningReplayEvictionEpoch uint64

	// antigravityReasoningReplayDiskRoot is the private cache directory for
	// standalone replay persistence. An empty root keeps the cache in-memory
	// only, preserving the pre-disk behavior for every mode until the service
	// explicitly configures a root.
	antigravityReasoningReplayDiskRoot string
)

type antigravityReasoningReplayKVClient interface {
	KVGet(ctx context.Context, key string) ([]byte, bool, error)
	KVSet(ctx context.Context, key string, value []byte, opts homekv.KVSetOptions) (bool, error)
	KVCompareAndSwap(ctx context.Context, key string, expected []byte, expectedExists bool, value []byte, ttl time.Duration) (bool, error)
	KVExpire(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

var currentAntigravityReasoningReplayKVClient = func() (antigravityReasoningReplayKVClient, bool, error) {
	return homekv.CurrentKVClient()
}

// SetAntigravityReasoningReplayCacheRoot configures the private directory used
// to persist standalone replay state across process restarts.
//
// [WHY]
// Standalone deployments have no Home KV store; a local cache root under the
// resolved authentication directory gives them restart continuity without
// changing the public cache API.
//
// [HOW]
// 1. Trim the supplied directory.
// 2. Store it under the cache lock; an empty value disables disk persistence.
//
// [RULES / NOTES]
// - Passing an empty root restores the original in-memory-only behavior.
// - The root must be absolute and private; callers derive it from AuthDir.
//
// @param dir absolute cache root (expected `<AuthDir>/antigravity-replay`).
func SetAntigravityReasoningReplayCacheRoot(dir string) {
	antigravityReasoningReplayMu.Lock()
	defer antigravityReasoningReplayMu.Unlock()
	antigravityReasoningReplayDiskRoot = strings.TrimSpace(dir)
}

// antigravityReasoningReplayDiskStoreFor returns the active disk store for
// standalone persistence, or nil when no cache root is configured.
//
// [WHY]
// All persistence hooks must silently no-op unless the service configured a
// private root, preserving Home mode and the original in-memory behavior.
//
// [HOW]
// 1. Reject an empty configured root.
// 2. Bind a store to the configured root directory.
//
// @return the disk store, or nil when persistence is disabled.
func antigravityReasoningReplayDiskStoreFor() *antigravityReasoningReplayDiskStore {
	if antigravityReasoningReplayDiskRoot == "" {
		return nil
	}
	return newAntigravityReasoningReplayDiskStore(antigravityReasoningReplayDiskRoot)
}

// antigravityReasoningReplayMirrorLocked persists one successful standalone
// mutation after the in-memory map was updated. antigravityReasoningReplayMu
// must be held by the caller.
//
// [WHY]
// Restart survival requires every accepted mutation to reach disk; a failed
// write must never fail the request, only leave a stale file behind.
//
// [HOW]
// 1. Skip when no disk store is configured.
// 2. Save the entry; log best-effort failures at debug level.
//
// [RULES / NOTES]
// - The entry was already normalized and limit-checked by the caller.
// - A failed save keeps the in-memory mutation authoritative.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param entry the mutated entry to persist.
func antigravityReasoningReplayMirrorLocked(modelName, sessionKey string, entry antigravityReasoningReplayEntry) {
	store := antigravityReasoningReplayDiskStoreFor()
	if store == nil {
		return
	}
	if errSave := store.save(modelName, sessionKey, entry); errSave != nil {
		log.Debugf("antigravity replay disk: mirror standalone write failed: %v", errSave)
	}
}

// antigravityReasoningReplayHydrateFromDiskLocked loads one persisted entry
// when the in-memory map lacks the key, populating the map so the normal read
// path and later mutations fence against the restored revision and branch.
// antigravityReasoningReplayMu must be held by the caller.
//
// [WHY]
// After a process restart only the disk holds replay state; hydration must
// reproduce the exact pre-restart entry, revision, and branch semantics.
//
// [HOW]
// 1. Skip when no disk store is configured.
// 2. Load the entry; every disk rejection is a miss.
// 3. Refresh the timestamp, insert the entry, and bound the map.
//
// [RULES / NOTES]
// - The disk store validates TTL, permissions, and limits before returning.
// - A miss must not fabricate provenance; callers reserve absence as usual.
//
// @param key the in-memory map key derived from the model/session pair.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param now the reference time for TTL refresh.
// @return the hydrated entry and true on success, otherwise the zero entry and false.
func antigravityReasoningReplayHydrateFromDiskLocked(key, modelName, sessionKey string, now time.Time) (antigravityReasoningReplayEntry, bool) {
	store := antigravityReasoningReplayDiskStoreFor()
	if store == nil {
		return antigravityReasoningReplayEntry{}, false
	}
	entry, found := store.load(modelName, sessionKey, now)
	if !found {
		return antigravityReasoningReplayEntry{}, false
	}
	entry.Timestamp = now
	antigravityReasoningReplayEntries[key] = entry
	if len(antigravityReasoningReplayEntries) > AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	return entry, true
}

// antigravityReasoningReplayDiskDeleteLocked removes the persisted file for
// one model/session key. antigravityReasoningReplayMu must be held by the
// caller.
//
// [WHY]
// Deletion and expiry must clear durable state so a restart cannot resurrect
// a cleared conversation or an expired chain.
//
// [HOW]
// 1. Skip when no disk store is configured.
// 2. Delete the file; log best-effort failures at debug level.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
func antigravityReasoningReplayDiskDeleteLocked(modelName, sessionKey string) {
	store := antigravityReasoningReplayDiskStoreFor()
	if store == nil {
		return
	}
	if errDelete := store.delete(modelName, sessionKey); errDelete != nil {
		log.Debugf("antigravity replay disk: delete standalone entry failed: %v", errDelete)
	}
}

// antigravityReasoningReplayDiskDeleteKeyLocked removes the persisted file
// for an expired in-memory key by parsing the model/session pair back out of
// the cache key. antigravityReasoningReplayMu must be held by the caller.
//
// [WHY]
// Background expiry cleanup iterates the map by key only; the persisted file
// is at least as old as the expired memory entry and must not linger.
//
// [HOW]
// 1. Split the key on the NUL separators used by the key derivation.
// 2. Delete the file for the recovered model/session pair.
//
// [RULES / NOTES]
//   - Keys that do not round-trip (pathological embedded NULs) skip removal;
//     their memory entry is still purged and a later load rejects the file.
//
// @param key the in-memory map key of the expired entry.
func antigravityReasoningReplayDiskDeleteKeyLocked(key string) {
	parts := strings.SplitN(key, "\x00", 3)
	if len(parts) != 3 || parts[0] != "antigravity-reasoning-replay" {
		return
	}
	antigravityReasoningReplayDiskDeleteLocked(parts[1], parts[2])
}

// CacheAntigravityReasoningReplayItem stores a final GPT/Codex reasoning item for
// stateless replay. The stored item is normalized to the minimal shape accepted
// by Responses input replay.
func CacheAntigravityReasoningReplayItem(modelName, sessionKey string, item []byte) bool {
	return CacheAntigravityReasoningReplayItems(modelName, sessionKey, [][]byte{item})
}

// CacheAntigravityReasoningReplayItems stores the final GPT/Codex assistant output
// items needed to replay a stateless next turn.
func CacheAntigravityReasoningReplayItems(modelName, sessionKey string, items [][]byte) bool {
	return CacheAntigravityReasoningReplayItemsBestEffort(context.Background(), modelName, sessionKey, items)
}

// CacheAntigravityReasoningReplayItemsBestEffort stores replay items for
// completed response paths.
//
// [WHY]
// A finished response chain must survive a restart so the next turn can replay
// it; standalone mode additionally mirrors the accepted entry to disk.
//
// [HOW]
// 1. Reject blank keys and unnormalizable item chains.
// 2. Prefer Home KV when the Home client is active.
// 3. Otherwise write the normalized entry to the in-memory map.
// 4. Mirror the successful mutation to the configured disk store.
//
// [RULES / NOTES]
// - A failed disk mirror never fails the write; memory stays authoritative.
//
// @param ctx the caller context for Home KV operations.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param items the raw reasoning items to normalize and store.
// @return true when the items were cached.
func CacheAntigravityReasoningReplayItemsBestEffort(ctx context.Context, modelName, sessionKey string, items [][]byte) bool {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return false
	}
	normalized, ok := normalizeAntigravityReasoningReplayItems(items)
	if !ok {
		return false
	}
	if client, homeMode, errClient := currentAntigravityReasoningReplayKVClient(); homeMode {
		if errClient != nil {
			log.Errorf("home kv best-effort antigravity reasoning replay set failed prefix=cpa:antigravity:*: %v", errClient)
			return false
		}
		raw, errMarshal := marshalAntigravityReasoningReplayHomeValue(normalized, "")
		if errMarshal != nil {
			log.Errorf("home kv best-effort antigravity reasoning replay set failed prefix=cpa:antigravity:*: %v", errMarshal)
			return false
		}
		written, errSet := client.KVSet(ctx, antigravityReasoningReplayKVKey(modelName, sessionKey), raw, homekv.KVSetOptions{EX: AntigravityReasoningReplayCacheTTL})
		if errSet != nil {
			log.Errorf("home kv best-effort antigravity reasoning replay set failed prefix=cpa:antigravity:*: %v", errSet)
			return false
		}
		return written
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	defer antigravityReasoningReplayMu.Unlock()
	antigravityReasoningReplayNextRevision++
	entry := antigravityReasoningReplayEntry{
		Items:     normalized,
		Timestamp: now,
		Revision:  antigravityReasoningReplayNextRevision,
		Branch:    newAntigravityReasoningReplayGeneration(),
	}
	antigravityReasoningReplayEntries[key] = entry
	if len(antigravityReasoningReplayEntries) > AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	antigravityReasoningReplayMirrorLocked(modelName, sessionKey, entry)
	return true
}

// GetAntigravityReasoningReplayItem retrieves a normalized reasoning replay item.
func GetAntigravityReasoningReplayItem(modelName, sessionKey string) ([]byte, bool) {
	items, ok := GetAntigravityReasoningReplayItems(modelName, sessionKey)
	if !ok || len(items) == 0 {
		return nil, false
	}
	return items[0], true
}

// GetAntigravityReasoningReplayItems retrieves normalized assistant output items.
func GetAntigravityReasoningReplayItems(modelName, sessionKey string) ([][]byte, bool) {
	items, ok, err := GetAntigravityReasoningReplayItemsRequired(context.Background(), modelName, sessionKey)
	if err == nil {
		return items, ok
	}
	return nil, false
}

// GetAntigravityReasoningReplayItemsRequired retrieves replay items for request-time paths.
func GetAntigravityReasoningReplayItemsRequired(ctx context.Context, modelName, sessionKey string) ([][]byte, bool, error) {
	items, _, found, errGet := GetAntigravityReasoningReplayItemsWithSnapshotRequired(ctx, modelName, sessionKey)
	return items, found, errGet
}

// GetAntigravityReasoningReplayItemsWithSnapshotRequired retrieves replay items
// and the exact cache state that guarded this request.
//
// [WHY]
// Request-time paths need both the items and the fencing snapshot so a later
// conditional mutation can prove it still owns the state; standalone reads
// hydrate from disk only when memory lost the key.
//
// [HOW]
// 1. Prefer Home KV when the Home client is active.
// 2. Serve from the in-memory map, refreshing the entry TTL.
// 3. On a memory miss, hydrate the key from the configured disk store.
// 4. On expiry, remove memory and persisted state and reserve absence.
//
// [RULES / NOTES]
// - Disk load failures are misses; they never fabricate provenance.
// - Hydration preserves the persisted revision and branch for fencing.
//
// @param ctx the caller context for Home KV operations.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @return the normalized items, the guarding snapshot, whether state was found, and an error.
func GetAntigravityReasoningReplayItemsWithSnapshotRequired(ctx context.Context, modelName, sessionKey string) ([][]byte, AntigravityReasoningReplaySnapshot, bool, error) {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil, AntigravityReasoningReplaySnapshot{}, false, nil
	}
	client, homeMode, errClient := currentAntigravityReasoningReplayKVClient()
	if homeMode {
		if errClient != nil {
			return nil, AntigravityReasoningReplaySnapshot{}, false, errClient
		}
		kvKey := antigravityReasoningReplayKVKey(modelName, sessionKey)
		var raw []byte
		found := false
		for attempt := 0; attempt < 4; attempt++ {
			currentRaw, currentFound, errGet := client.KVGet(ctx, kvKey)
			if errGet != nil {
				return nil, AntigravityReasoningReplaySnapshot{loaded: true}, false, errGet
			}
			if currentFound {
				raw = currentRaw
				found = true
				break
			}
			reservation := newAntigravityReasoningReplayTombstone()
			swapped, errReserve := client.KVCompareAndSwap(ctx, kvKey, nil, false, reservation, AntigravityReasoningReplayCacheTTL)
			if errReserve != nil {
				return nil, AntigravityReasoningReplaySnapshot{loaded: true}, false, errReserve
			}
			if swapped {
				raw = reservation
				found = true
				break
			}
		}
		if !found {
			return nil, AntigravityReasoningReplaySnapshot{loaded: true}, false, fmt.Errorf("could not fence absent antigravity reasoning replay state")
		}
		if len(raw) > antigravityReasoningReplayCacheMaxSerializedBytes {
			return nil, AntigravityReasoningReplaySnapshot{loaded: true, found: true}, false, nil
		}
		snapshot := AntigravityReasoningReplaySnapshot{raw: append([]byte(nil), raw...), loaded: true, found: true}
		homeItems, deleted, _, branch, okDecode := decodeAntigravityReasoningReplayHomeValue(raw)
		snapshot.branch = branch
		if !okDecode || deleted || len(homeItems) == 0 {
			return nil, snapshot, false, nil
		}
		if len(homeItems) > AntigravityReasoningReplayCacheMaxItemsPerEntry {
			return nil, snapshot, false, nil
		}
		normalized, okNormalize := normalizeAntigravityReasoningReplayItems(homeItems)
		if !okNormalize || len(normalized) != len(homeItems) {
			return nil, snapshot, false, nil
		}
		snapshot.items = cloneAntigravityReasoningReplayItems(normalized)
		if _, errExpire := client.KVExpire(ctx, kvKey, AntigravityReasoningReplayCacheTTL); errExpire != nil {
			return nil, snapshot, false, errExpire
		}
		return normalized, snapshot, true, nil
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	defer antigravityReasoningReplayMu.Unlock()
	entry, ok := antigravityReasoningReplayEntries[key]
	if !ok {
		entry, ok = antigravityReasoningReplayHydrateFromDiskLocked(key, modelName, sessionKey, now)
	}
	if !ok {
		return nil, reserveAntigravityReasoningReplayAbsentLocked(key, now), false, nil
	}
	if now.Sub(entry.Timestamp) > AntigravityReasoningReplayCacheTTL {
		antigravityReasoningReplayEvictionEpoch++
		delete(antigravityReasoningReplayEntries, key)
		antigravityReasoningReplayDiskDeleteLocked(modelName, sessionKey)
		return nil, reserveAntigravityReasoningReplayAbsentLocked(key, now), false, nil
	}
	entry.Timestamp = now
	antigravityReasoningReplayEntries[key] = entry
	snapshot := AntigravityReasoningReplaySnapshot{loaded: true, found: true, revision: entry.Revision, branch: entry.Branch, evictionEpoch: antigravityReasoningReplayEvictionEpoch}
	if entry.Deleted || len(entry.Items) == 0 {
		return nil, snapshot, false, nil
	}
	snapshot.items = cloneAntigravityReasoningReplayItems(entry.Items)
	return cloneAntigravityReasoningReplayItems(entry.Items), snapshot, true, nil
}

// reserveAntigravityReasoningReplayAbsentLocked fences a local miss with a
// per-key tombstone so eviction of an unrelated key cannot invalidate it.
// antigravityReasoningReplayMu must be held by the caller.
func reserveAntigravityReasoningReplayAbsentLocked(key string, now time.Time) AntigravityReasoningReplaySnapshot {
	if len(antigravityReasoningReplayEntries) >= AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	antigravityReasoningReplayNextRevision++
	entry := antigravityReasoningReplayEntry{
		Timestamp: now,
		Revision:  antigravityReasoningReplayNextRevision,
		Branch:    newAntigravityReasoningReplayGeneration(),
		Deleted:   true,
	}
	antigravityReasoningReplayEntries[key] = entry
	return AntigravityReasoningReplaySnapshot{
		loaded:        true,
		found:         true,
		revision:      entry.Revision,
		branch:        entry.Branch,
		evictionEpoch: antigravityReasoningReplayEvictionEpoch,
	}
}

// ReplaceAntigravityReasoningReplayItemsIfUnchanged publishes a completed chain
// only when no newer request has changed the state read by this request.
//
// [WHY]
// Concurrent turns must never overwrite newer state with a stale chain;
// standalone accepted replacements are mirrored to disk for restart survival.
//
// [HOW]
// 1. Validate and normalize the replacement items.
// 2. Prefer Home KV compare-and-swap when the Home client is active.
// 3. Fence the standalone write against the snapshot revision and branch.
// 4. On success, replace the memory entry and mirror it to disk.
//
// [RULES / NOTES]
// - A failed disk mirror never fails the write; memory stays authoritative.
//
// @param ctx the caller context for Home KV operations.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param snapshot the state guard captured when this chain was read.
// @param items the raw reasoning items to normalize and store.
// @return true when the replacement was applied, and any error.
func ReplaceAntigravityReasoningReplayItemsIfUnchanged(ctx context.Context, modelName, sessionKey string, snapshot AntigravityReasoningReplaySnapshot, items [][]byte) (bool, error) {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return false, nil
	}
	normalized, okNormalize := normalizeAntigravityReasoningReplayItems(items)
	if !okNormalize {
		return false, fmt.Errorf("invalid antigravity reasoning replay items")
	}
	if !snapshot.loaded {
		return CacheAntigravityReasoningReplayItemsBestEffort(ctx, modelName, sessionKey, normalized), nil
	}
	client, homeMode, errClient := currentAntigravityReasoningReplayKVClient()
	if homeMode {
		if errClient != nil {
			return false, errClient
		}
		kvKey := antigravityReasoningReplayKVKey(modelName, sessionKey)
		expectedRaw := snapshot.raw
		expectedFound := snapshot.found
		branch := snapshot.branch
		if branch == "" || !antigravityReasoningReplayItemsPrefix(snapshot.items, normalized) {
			branch = newAntigravityReasoningReplayGeneration()
		}
		for attempt := 0; attempt < 4; attempt++ {
			raw, errMarshal := marshalAntigravityReasoningReplayHomeValue(normalized, branch)
			if errMarshal != nil {
				return false, errMarshal
			}
			swapped, errCAS := client.KVCompareAndSwap(ctx, kvKey, expectedRaw, expectedFound, raw, AntigravityReasoningReplayCacheTTL)
			if errCAS != nil || swapped {
				return swapped, errCAS
			}
			currentRaw, currentFound, errGet := client.KVGet(ctx, kvKey)
			if errGet != nil || !currentFound {
				return false, errGet
			}
			if len(currentRaw) > antigravityReasoningReplayCacheMaxSerializedBytes {
				return false, nil
			}
			currentItems, deleted, _, currentBranch, okDecode := decodeAntigravityReasoningReplayHomeValue(currentRaw)
			if !okDecode || deleted || snapshot.branch == "" || currentBranch != snapshot.branch {
				return false, nil
			}
			normalizedCurrent, okNormalizeCurrent := normalizeAntigravityReasoningReplayItems(currentItems)
			if !okNormalizeCurrent || len(normalizedCurrent) != len(currentItems) || !antigravityReasoningReplayItemsPrefix(normalizedCurrent, normalized) {
				return false, nil
			}
			expectedRaw = currentRaw
			expectedFound = true
		}
		return false, nil
	}

	cacheCleanupOnce.Do(startCacheCleanup)
	now := time.Now()
	antigravityReasoningReplayMu.Lock()
	defer antigravityReasoningReplayMu.Unlock()
	entry, found := antigravityReasoningReplayEntries[key]
	matchesSnapshot := found == snapshot.found && ((found && entry.Revision == snapshot.revision) || (!found && snapshot.evictionEpoch == antigravityReasoningReplayEvictionEpoch))
	isDescendant := found && !entry.Deleted && snapshot.branch != "" && entry.Branch == snapshot.branch && antigravityReasoningReplayItemsPrefix(entry.Items, normalized)
	if !matchesSnapshot && !isDescendant {
		return false, nil
	}
	branch := snapshot.branch
	if branch == "" || (matchesSnapshot && !antigravityReasoningReplayItemsPrefix(snapshot.items, normalized)) {
		branch = newAntigravityReasoningReplayGeneration()
	}
	antigravityReasoningReplayNextRevision++
	entry = antigravityReasoningReplayEntry{Items: normalized, Timestamp: now, Revision: antigravityReasoningReplayNextRevision, Branch: branch}
	antigravityReasoningReplayEntries[key] = entry
	if len(antigravityReasoningReplayEntries) > AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	antigravityReasoningReplayMirrorLocked(modelName, sessionKey, entry)
	return true, nil
}

// DeleteAntigravityReasoningReplayItemsIfUnchanged clears replay state only when
// it still matches the state read for this request.
//
// [WHY]
// A stale request must never delete a newer chain; a successful standalone
// delete also removes the persisted file so restart cannot resurrect it.
//
// [HOW]
// 1. Prefer Home KV compare-and-swap when the Home client is active.
// 2. Fence the standalone delete against the snapshot revision and epoch.
// 3. On success, tombstone the memory entry and remove the disk file.
//
// [RULES / NOTES]
// - The in-memory tombstone keeps in-process fencing; the disk file is removed.
//
// @param ctx the caller context for Home KV operations.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param snapshot the state guard captured when this chain was read.
// @return true when the deletion was applied, and any error.
func DeleteAntigravityReasoningReplayItemsIfUnchanged(ctx context.Context, modelName, sessionKey string, snapshot AntigravityReasoningReplaySnapshot) (bool, error) {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return false, nil
	}
	if !snapshot.loaded {
		return true, DeleteAntigravityReasoningReplayItemRequired(ctx, modelName, sessionKey)
	}
	client, homeMode, errClient := currentAntigravityReasoningReplayKVClient()
	if homeMode {
		if errClient != nil {
			return false, errClient
		}
		return client.KVCompareAndSwap(ctx, antigravityReasoningReplayKVKey(modelName, sessionKey), snapshot.raw, snapshot.found, newAntigravityReasoningReplayTombstone(), AntigravityReasoningReplayCacheTTL)
	}
	cacheCleanupOnce.Do(startCacheCleanup)
	antigravityReasoningReplayMu.Lock()
	defer antigravityReasoningReplayMu.Unlock()
	entry, found := antigravityReasoningReplayEntries[key]
	if found != snapshot.found || (found && entry.Revision != snapshot.revision) || (!found && snapshot.evictionEpoch != antigravityReasoningReplayEvictionEpoch) {
		return false, nil
	}
	antigravityReasoningReplayNextRevision++
	antigravityReasoningReplayEntries[key] = antigravityReasoningReplayEntry{Timestamp: time.Now(), Revision: antigravityReasoningReplayNextRevision, Branch: newAntigravityReasoningReplayGeneration(), Deleted: true}
	if len(antigravityReasoningReplayEntries) > AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	antigravityReasoningReplayDiskDeleteLocked(modelName, sessionKey)
	return true, nil
}

// DeleteAntigravityReasoningReplayItem removes one replay item after upstream rejects
// it or the caller otherwise knows it is stale.
func DeleteAntigravityReasoningReplayItem(modelName, sessionKey string) {
	if errDelete := DeleteAntigravityReasoningReplayItemRequired(context.Background(), modelName, sessionKey); errDelete != nil {
		return
	}
}

// DeleteAntigravityReasoningReplayItemRequired removes one replay item for
// request-time paths.
//
// [WHY]
// Rejected or stale provenance must not be replayed; standalone deletion also
// removes the persisted file so a restart cannot resurrect the entry.
//
// [HOW]
// 1. Prefer a Home KV tombstone when the Home client is active.
// 2. Tombstone the in-memory entry.
// 3. Remove the persisted file best-effort.
//
// [RULES / NOTES]
// - Disk removal failures are non-fatal and logged at debug level.
//
// @param ctx the caller context for Home KV operations.
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @return any error from Home KV or the local tombstone write.
func DeleteAntigravityReasoningReplayItemRequired(ctx context.Context, modelName, sessionKey string) error {
	key := antigravityReasoningReplayCacheKey(modelName, sessionKey)
	if key == "" {
		return nil
	}
	client, homeMode, errClient := currentAntigravityReasoningReplayKVClient()
	if homeMode {
		if errClient != nil {
			return errClient
		}
		_, errSet := client.KVSet(ctx, antigravityReasoningReplayKVKey(modelName, sessionKey), newAntigravityReasoningReplayTombstone(), homekv.KVSetOptions{EX: AntigravityReasoningReplayCacheTTL})
		return errSet
	}
	cacheCleanupOnce.Do(startCacheCleanup)
	antigravityReasoningReplayMu.Lock()
	antigravityReasoningReplayNextRevision++
	antigravityReasoningReplayEntries[key] = antigravityReasoningReplayEntry{Timestamp: time.Now(), Revision: antigravityReasoningReplayNextRevision, Branch: newAntigravityReasoningReplayGeneration(), Deleted: true}
	if len(antigravityReasoningReplayEntries) > AntigravityReasoningReplayCacheMaxEntries {
		evictOldestAntigravityReasoningReplayEntries(AntigravityReasoningReplayCacheEvictBatchSize)
	}
	antigravityReasoningReplayDiskDeleteLocked(modelName, sessionKey)
	antigravityReasoningReplayMu.Unlock()
	return nil
}

func newAntigravityReasoningReplayGeneration() string {
	var nonce [16]byte
	if _, errRead := rand.Read(nonce[:]); errRead != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("%x", nonce[:])
}

func marshalAntigravityReasoningReplayHomeValue(items [][]byte, branch string) ([]byte, error) {
	if branch == "" {
		branch = newAntigravityReasoningReplayGeneration()
	}
	marker := []byte(`{"type":"","generation":"","branch":""}`)
	marker, _ = sjson.SetBytes(marker, "type", antigravityReasoningReplayGenerationItemType)
	marker, _ = sjson.SetBytes(marker, "generation", newAntigravityReasoningReplayGeneration())
	marker, _ = sjson.SetBytes(marker, "branch", branch)
	stored := make([][]byte, 0, len(items)+1)
	stored = append(stored, marker)
	stored = append(stored, items...)
	return json.Marshal(stored)
}

func decodeAntigravityReasoningReplayHomeValue(raw []byte) (items [][]byte, deleted bool, generation, branch string, ok bool) {
	if errUnmarshal := json.Unmarshal(raw, &items); errUnmarshal != nil {
		return nil, false, "", "", false
	}
	if len(items) == 0 || strings.TrimSpace(gjson.GetBytes(items[0], "type").String()) != antigravityReasoningReplayGenerationItemType {
		return items, false, "", "", true
	}
	marker := gjson.ParseBytes(items[0])
	deleted = marker.Get("deleted").Bool()
	generation = strings.TrimSpace(marker.Get("generation").String())
	branch = strings.TrimSpace(marker.Get("branch").String())
	return items[1:], deleted, generation, branch, true
}

func antigravityReasoningReplayItemsPrefix(prefix, items [][]byte) bool {
	if len(prefix) > len(items) {
		return false
	}
	for index := range prefix {
		if !bytes.Equal(prefix[index], items[index]) {
			return false
		}
	}
	return true
}

func newAntigravityReasoningReplayTombstone() []byte {
	marker := []byte(`{"type":"","generation":"","branch":"","deleted":true}`)
	marker, _ = sjson.SetBytes(marker, "type", antigravityReasoningReplayGenerationItemType)
	marker, _ = sjson.SetBytes(marker, "generation", newAntigravityReasoningReplayGeneration())
	marker, _ = sjson.SetBytes(marker, "branch", newAntigravityReasoningReplayGeneration())
	raw, _ := json.Marshal([][]byte{marker})
	return raw
}

// ClearAntigravityReasoningReplayCache clears all Antigravity reasoning replay
// state, including any persisted standalone files.
//
// [WHY]
// Cache-wide resets must also discard durable state; operators rely on this
// call (or removing the cache directory) to drop replay provenance.
//
// [HOW]
// 1. Reset the in-memory map and bump the eviction epoch.
// 2. Remove the whole disk store directory when one is configured.
//
// [RULES / NOTES]
// - Directory removal failures are non-fatal and logged at debug level.
func ClearAntigravityReasoningReplayCache() {
	antigravityReasoningReplayMu.Lock()
	antigravityReasoningReplayEntries = make(map[string]antigravityReasoningReplayEntry)
	antigravityReasoningReplayEvictionEpoch++
	store := antigravityReasoningReplayDiskStoreFor()
	antigravityReasoningReplayMu.Unlock()
	if store != nil {
		if errClear := store.clear(); errClear != nil {
			log.Debugf("antigravity replay disk: clear standalone persistence: %v", errClear)
		}
	}
}

func antigravityReasoningReplayCacheKey(modelName, sessionKey string) string {
	modelName = strings.TrimSpace(modelName)
	sessionKey = strings.TrimSpace(sessionKey)
	if modelName == "" || sessionKey == "" {
		return ""
	}
	// The session key is the continuity boundary. Keep this independent from
	// the selected upstream Codex credential so auth failover can preserve replay.
	return strings.Join([]string{"antigravity-reasoning-replay", modelName, sessionKey}, "\x00")
}

func antigravityReasoningReplayKVKey(modelName, sessionKey string) string {
	return "cpa:antigravity:reasoning-replay:" + homekv.HashKeyPart(strings.TrimSpace(modelName)) + ":" + homekv.HashKeyPart(strings.TrimSpace(sessionKey))
}

func normalizeAntigravityReasoningReplayItems(items [][]byte) ([][]byte, bool) {
	if len(items) > AntigravityReasoningReplayCacheMaxItemsPerEntry {
		return nil, false
	}
	normalized := make([][]byte, 0, len(items))
	totalBytes := 0
	for _, item := range items {
		normalizedItem, ok := normalizeAntigravityReasoningReplayItem(item)
		if ok {
			totalBytes += len(normalizedItem)
			if totalBytes > AntigravityReasoningReplayCacheMaxBytesPerEntry {
				return nil, false
			}
			normalized = append(normalized, normalizedItem)
		}
	}
	return normalized, len(normalized) > 0
}

func normalizeAntigravityReasoningReplayItem(item []byte) ([]byte, bool) {
	itemResult := gjson.ParseBytes(item)
	switch strings.TrimSpace(itemResult.Get("type").String()) {
	case "thought_signature":
		return normalizeAntigravityThoughtSignatureReplayItem(itemResult)
	case "function_call_part":
		return normalizeAntigravityFunctionCallPartReplayItem(itemResult)
	default:
		return nil, false
	}
}

func normalizeAntigravityThoughtSignatureReplayItem(itemResult gjson.Result) ([]byte, bool) {
	sig := strings.TrimSpace(itemResult.Get("thoughtSignature").String())
	if sig == "" {
		sig = strings.TrimSpace(itemResult.Get("thought_signature").String())
	}
	if sig == "" || sig == "skip_thought_signature_validator" || len(sig) < minAntigravityThoughtSignatureReplayLen {
		return nil, false
	}
	normalized := []byte(`{"type":"thought_signature"}`)
	normalized, _ = sjson.SetBytes(normalized, "thoughtSignature", sig)
	if contentIndex := itemResult.Get("contentIndex"); contentIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "contentIndex", contentIndex.Int())
	}
	if partIndex := itemResult.Get("partIndex"); partIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "partIndex", partIndex.Int())
	}
	if targetKind := strings.TrimSpace(itemResult.Get("targetKind").String()); targetKind == "text" || targetKind == "thought" {
		normalized, _ = sjson.SetBytes(normalized, "targetKind", targetKind)
	}
	if targetHash := strings.TrimSpace(itemResult.Get("targetHash").String()); targetHash != "" {
		normalized, _ = sjson.SetBytes(normalized, "targetHash", targetHash)
	}
	if targetOccurrence := itemResult.Get("targetOccurrence"); targetOccurrence.Type == gjson.Number && targetOccurrence.Int() >= 0 {
		normalized, _ = sjson.SetBytes(normalized, "targetOccurrence", targetOccurrence.Int())
	}
	if contextHash := strings.TrimSpace(itemResult.Get("contextHash").String()); contextHash != "" {
		normalized, _ = sjson.SetBytes(normalized, "contextHash", contextHash)
	}
	return normalized, true
}

func normalizeAntigravityFunctionCallPartReplayItem(itemResult gjson.Result) ([]byte, bool) {
	callID := strings.TrimSpace(itemResult.Get("call_id").String())
	if callID == "" {
		callID = strings.TrimSpace(itemResult.Get("id").String())
	}
	name := strings.TrimSpace(itemResult.Get("name").String())
	args := itemResult.Get("args")
	if name == "" || !args.Exists() {
		fc := itemResult.Get("functionCall")
		if fc.Exists() {
			if callID == "" {
				callID = strings.TrimSpace(fc.Get("id").String())
			}
			if name == "" {
				name = strings.TrimSpace(fc.Get("name").String())
			}
			if !args.Exists() {
				args = fc.Get("args")
			}
		}
	}
	if name == "" || !args.Exists() {
		return nil, false
	}
	normalized := []byte(`{"type":"function_call_part"}`)
	if callID != "" {
		normalized, _ = sjson.SetBytes(normalized, "call_id", callID)
	}
	normalized, _ = sjson.SetBytes(normalized, "name", name)
	if args.Type == gjson.String {
		normalized, _ = sjson.SetBytes(normalized, "args", args.String())
	} else {
		normalized, _ = sjson.SetRawBytes(normalized, "args", []byte(args.Raw))
	}
	sig := strings.TrimSpace(itemResult.Get("thoughtSignature").String())
	if sig != "" && sig != "skip_thought_signature_validator" {
		normalized, _ = sjson.SetBytes(normalized, "thoughtSignature", sig)
	}
	if contentIndex := itemResult.Get("contentIndex"); contentIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "contentIndex", contentIndex.Int())
	}
	if partIndex := itemResult.Get("partIndex"); partIndex.Type == gjson.Number {
		normalized, _ = sjson.SetBytes(normalized, "partIndex", partIndex.Int())
	}
	if targetOccurrence := itemResult.Get("targetOccurrence"); targetOccurrence.Type == gjson.Number && targetOccurrence.Int() >= 0 {
		normalized, _ = sjson.SetBytes(normalized, "targetOccurrence", targetOccurrence.Int())
	}
	if contextHash := strings.TrimSpace(itemResult.Get("contextHash").String()); contextHash != "" {
		normalized, _ = sjson.SetBytes(normalized, "contextHash", contextHash)
	}
	return normalized, true
}

func cloneAntigravityReasoningReplayItems(items [][]byte) [][]byte {
	cloned := make([][]byte, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, append([]byte(nil), item...))
	}
	return cloned
}

func evictOldestAntigravityReasoningReplayEntries(count int) {
	if count <= 0 || len(antigravityReasoningReplayEntries) == 0 {
		return
	}
	type candidate struct {
		key       string
		timestamp time.Time
	}
	candidates := make([]candidate, 0, len(antigravityReasoningReplayEntries))
	for key, entry := range antigravityReasoningReplayEntries {
		candidates = append(candidates, candidate{key: key, timestamp: entry.Timestamp})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].timestamp.Before(candidates[j].timestamp)
	})
	if count > len(candidates) {
		count = len(candidates)
	}
	for i := 0; i < count; i++ {
		antigravityReasoningReplayEvictionEpoch++
		delete(antigravityReasoningReplayEntries, candidates[i].key)
	}
}

func purgeExpiredAntigravityReasoningReplayCache(now time.Time) {
	antigravityReasoningReplayMu.Lock()
	for key, entry := range antigravityReasoningReplayEntries {
		if now.Sub(entry.Timestamp) > AntigravityReasoningReplayCacheTTL {
			antigravityReasoningReplayEvictionEpoch++
			delete(antigravityReasoningReplayEntries, key)
			antigravityReasoningReplayDiskDeleteKeyLocked(key)
		}
	}
	antigravityReasoningReplayMu.Unlock()
}
