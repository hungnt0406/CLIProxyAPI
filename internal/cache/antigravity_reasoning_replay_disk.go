package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// antigravityReasoningReplayDiskStore persists one normalized replay entry per
// model/session key below a private cache directory, so standalone replay
// provenance survives CLIProxyAPI restarts without Home KV infrastructure.
type antigravityReasoningReplayDiskStore struct {
	dir string
}

// antigravityReasoningReplayDiskFile is the on-disk JSON shape. It encodes only
// the replay entry fields, never original requests, responses, or transcripts.
type antigravityReasoningReplayDiskFile struct {
	Items     [][]byte  `json:"items"`
	Timestamp time.Time `json:"timestamp"`
	Revision  uint64    `json:"revision"`
	Branch    string    `json:"branch"`
	Deleted   bool      `json:"deleted"`
}

// newAntigravityReasoningReplayDiskStore creates a store rooted at dir. The
// directory is created lazily on the first save with owner-only permissions.
//
// [WHY]
// Standalone deployments need replay continuity across restarts; the store is
// the persistence primitive the in-memory cache mirrors to disk.
//
// [HOW]
// 1. Retain the configured root directory.
// 2. Return the store handle.
//
// @param dir absolute cache root (expected `<AuthDir>/antigravity-replay`).
// @return a disk store bound to dir.
func newAntigravityReasoningReplayDiskStore(dir string) *antigravityReasoningReplayDiskStore {
	return &antigravityReasoningReplayDiskStore{dir: dir}
}

// antigravityReasoningReplayDiskFileName derives a safe file name for one
// model/session key: the lowercase hex SHA-256 digest of
// modelName + "\x00" + sessionKey plus the ".json" suffix. Raw key text never
// appears in the path.
//
// [WHY]
// Model and session keys are user-controlled and must stay isolated; hashing
// keeps them out of file-system paths and prevents traversal.
//
// [HOW]
// 1. Trim and reject blank model or session values.
// 2. Hash the NUL-separated pair with SHA-256.
// 3. Produce the hex digest and return it with the JSON suffix.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @return the safe file name, and false when either key is blank.
func antigravityReasoningReplayDiskFileName(modelName, sessionKey string) (string, bool) {
	modelName = strings.TrimSpace(modelName)
	sessionKey = strings.TrimSpace(sessionKey)
	if modelName == "" || sessionKey == "" {
		return "", false
	}
	digest := sha256.Sum256([]byte(modelName + "\x00" + sessionKey))
	return hex.EncodeToString(digest[:]) + ".json", true
}

// pathFor resolves the absolute path for one model/session key inside the
// store directory.
//
// [WHY]
// Centralizes key-to-path resolution so save, load, and delete agree on the
// exact file location.
//
// [HOW]
// 1. Derive the hashed file name.
// 2. Join it under the store root.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @return the resolved path, and false when either key is blank.
func (s *antigravityReasoningReplayDiskStore) pathFor(modelName, sessionKey string) (string, bool) {
	fileName, ok := antigravityReasoningReplayDiskFileName(modelName, sessionKey)
	if !ok {
		return "", false
	}
	return filepath.Join(s.dir, fileName), true
}

// antigravityReasoningReplayDiskEntryValid reports whether an entry fits the
// shared per-entry item and byte limits, keeping serialized writes bounded.
//
// [WHY]
// Oversized chains must not be partially persisted; the same limits that bound
// memory also bound the disk.
//
// [HOW]
// 1. Reject more items than the per-entry item limit.
// 2. Reject a total item payload above the per-entry byte limit.
//
// @param entry the replay entry to validate.
// @return true when the entry fits the per-entry limits.
func antigravityReasoningReplayDiskEntryValid(entry antigravityReasoningReplayEntry) bool {
	if len(entry.Items) > AntigravityReasoningReplayCacheMaxItemsPerEntry {
		return false
	}
	total := 0
	for _, item := range entry.Items {
		total += len(item)
		if total > AntigravityReasoningReplayCacheMaxBytesPerEntry {
			return false
		}
	}
	return true
}

// save atomically persists one entry for a model/session key.
//
// [WHY]
// Restart survival requires durable, crash-consistent writes; a temp file in
// the same directory plus atomic rename guarantees readers never observe a
// partially written entry.
//
// [HOW]
// 1. Reject blank keys and entries exceeding the per-entry limits.
// 2. Create the cache directory with 0700 permissions.
// 3. JSON-encode only the replay entry fields.
// 4. Write to a same-directory 0600 temp file, Sync, and close.
// 5. Atomically rename the temp file over the target path.
//
// [RULES / NOTES]
// - A failed write leaves any previously persisted entry untouched.
// - The temp file is removed best-effort on every failure path.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param entry the normalized replay entry to persist.
// @return an error describing the failed save, or nil on success.
func (s *antigravityReasoningReplayDiskStore) save(modelName, sessionKey string, entry antigravityReasoningReplayEntry) error {
	fileName, ok := antigravityReasoningReplayDiskFileName(modelName, sessionKey)
	if !ok {
		return fmt.Errorf("antigravity replay disk save: empty model or session key")
	}
	if !antigravityReasoningReplayDiskEntryValid(entry) {
		return fmt.Errorf("antigravity replay disk save: entry exceeds per-entry item or byte limits")
	}
	if errMkdir := os.MkdirAll(s.dir, 0o700); errMkdir != nil {
		return fmt.Errorf("antigravity replay disk save: create cache directory: %w", errMkdir)
	}
	data, errMarshal := json.Marshal(antigravityReasoningReplayDiskFile{
		Items:     entry.Items,
		Timestamp: entry.Timestamp,
		Revision:  entry.Revision,
		Branch:    entry.Branch,
		Deleted:   entry.Deleted,
	})
	if errMarshal != nil {
		return fmt.Errorf("antigravity replay disk save: encode entry: %w", errMarshal)
	}
	tmp, errTemp := os.CreateTemp(s.dir, ".antigravity-replay-*.tmp")
	if errTemp != nil {
		return fmt.Errorf("antigravity replay disk save: create temp file: %w", errTemp)
	}
	tmpName := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpName)
		}
	}()
	if _, errWrite := tmp.Write(data); errWrite != nil {
		_ = tmp.Close()
		return fmt.Errorf("antigravity replay disk save: write temp file: %w", errWrite)
	}
	errSync := tmp.Sync()
	errClose := tmp.Close()
	if errSync != nil {
		return fmt.Errorf("antigravity replay disk save: sync temp file: %w", errSync)
	}
	if errClose != nil {
		return fmt.Errorf("antigravity replay disk save: close temp file: %w", errClose)
	}
	if errRename := os.Rename(tmpName, filepath.Join(s.dir, fileName)); errRename != nil {
		return fmt.Errorf("antigravity replay disk save: replace entry: %w", errRename)
	}
	removeTmp = false
	return nil
}

// load reads one entry for a model/session key, validating every unsafe
// condition before the entry is trusted.
//
// [WHY]
// Corrupt, expired, oversized, or world-accessible files must never become
// valid provenance; replay validation fails closed.
//
// [HOW]
// 1. Resolve the hashed path and reject blank keys.
// 2. Reject symlinks, non-regular files, and files with group/other bits.
// 3. Reject files larger than the serialized byte bound.
// 4. Strictly decode the entry JSON, rejecting unknown fields and trailing data.
// 5. Reject entries over the item or byte limits.
// 6. Reject entries older than the one-hour TTL.
// 7. Remove rejected files best-effort and return a miss.
//
// [RULES / NOTES]
// - Every rejection is a miss, never an error that can fabricate provenance.
// - A missing file is a plain miss.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @param now the reference time for TTL validation.
// @return the loaded entry and true on success, otherwise the zero entry and false.
func (s *antigravityReasoningReplayDiskStore) load(modelName, sessionKey string, now time.Time) (antigravityReasoningReplayEntry, bool) {
	path, ok := s.pathFor(modelName, sessionKey)
	if !ok {
		return antigravityReasoningReplayEntry{}, false
	}
	info, errStat := os.Lstat(path)
	if errStat != nil {
		return antigravityReasoningReplayEntry{}, false
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	if info.Size() > antigravityReasoningReplayCacheMaxSerializedBytes {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return antigravityReasoningReplayEntry{}, false
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, antigravityReasoningReplayCacheMaxSerializedBytes+1))
	decoder.DisallowUnknownFields()
	var fileEntry antigravityReasoningReplayDiskFile
	if errDecode := decoder.Decode(&fileEntry); errDecode != nil {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	if decoder.More() {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	var trailing any
	if errTrailing := decoder.Decode(&trailing); errTrailing != io.EOF {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	if !antigravityReasoningReplayDiskEntryValid(antigravityReasoningReplayEntry{Items: fileEntry.Items}) {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	if fileEntry.Timestamp.IsZero() || now.Sub(fileEntry.Timestamp) > AntigravityReasoningReplayCacheTTL {
		antigravityReasoningReplayDiskRemove(path)
		return antigravityReasoningReplayEntry{}, false
	}
	clonedItems := make([][]byte, len(fileEntry.Items))
	for index, item := range fileEntry.Items {
		clonedItems[index] = append([]byte(nil), item...)
	}
	return antigravityReasoningReplayEntry{
		Items:     clonedItems,
		Timestamp: fileEntry.Timestamp,
		Revision:  fileEntry.Revision,
		Branch:    fileEntry.Branch,
		Deleted:   fileEntry.Deleted,
	}, true
}

// delete removes the persisted entry for one model/session key.
//
// [WHY]
// Deletion must clear durable state so a restart cannot resurrect a cleared
// conversation.
//
// [HOW]
// 1. Resolve the hashed path and reject blank keys.
// 2. Remove the file, treating a missing file as success.
//
// @param modelName the upstream model name.
// @param sessionKey the conversation continuity boundary.
// @return an error describing the failed removal, or nil on success.
func (s *antigravityReasoningReplayDiskStore) delete(modelName, sessionKey string) error {
	path, ok := s.pathFor(modelName, sessionKey)
	if !ok {
		return fmt.Errorf("antigravity replay disk delete: empty model or session key")
	}
	if errRemove := os.Remove(path); errRemove != nil && !os.IsNotExist(errRemove) {
		return fmt.Errorf("antigravity replay disk delete: %w", errRemove)
	}
	return nil
}

// clear removes the whole cache directory, wiping every persisted entry.
//
// [WHY]
// Cache-wide resets must also clear durable state; operators rely on removing
// this directory to discard replay provenance.
//
// [HOW]
// 1. Recursively remove the store directory.
// 2. Treat a missing directory as success.
//
// @return an error describing the failed removal, or nil on success.
func (s *antigravityReasoningReplayDiskStore) clear() error {
	if errRemove := os.RemoveAll(s.dir); errRemove != nil {
		return fmt.Errorf("antigravity replay disk clear: %w", errRemove)
	}
	return nil
}

// antigravityReasoningReplayDiskRemove best-effort deletes a rejected cache
// file so corrupt or expired state cannot keep accumulating.
//
// [WHY]
// Cleanup must never mask the miss result or fail the read.
//
// [HOW]
// 1. Remove the file.
// 2. Ignore missing files, log other failures at debug level.
//
// @param path the file to remove.
func antigravityReasoningReplayDiskRemove(path string) {
	if errRemove := os.Remove(path); errRemove != nil && !os.IsNotExist(errRemove) {
		log.Debugf("antigravity replay disk: remove rejected cache file %s: %v", path, errRemove)
	}
}
