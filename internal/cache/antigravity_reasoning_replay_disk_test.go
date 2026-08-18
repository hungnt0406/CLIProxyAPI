package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func antigravityReplayDiskTestEntry(timestamp time.Time, deleted bool) antigravityReasoningReplayEntry {
	return antigravityReasoningReplayEntry{
		Items: [][]byte{
			[]byte(`{"type":"thought_signature","thoughtSignature":"disk-roundtrip-signature-1234567890"}`),
			[]byte(`{"type":"function_call_part","name":"run","args":{"cmd":"true"}}`),
		},
		Timestamp: timestamp,
		Revision:  42,
		Branch:    "disk-branch-abc123",
		Deleted:   deleted,
	}
}

func TestAntigravityReplayDiskRoundTripFreshStore(t *testing.T) {
	root := t.TempDir()
	const model, session = "gemini-3.6-flash-high", "../sessions/private-123"
	entry := antigravityReplayDiskTestEntry(time.Now().Truncate(time.Millisecond), false)

	first := newAntigravityReasoningReplayDiskStore(root)
	if errSave := first.save(model, session, entry); errSave != nil {
		t.Fatalf("initial save failed: %v", errSave)
	}

	fresh := newAntigravityReasoningReplayDiskStore(root)
	loaded, found := fresh.load(model, session, time.Now())
	if !found {
		t.Fatal("fresh store did not load persisted entry")
	}
	if len(loaded.Items) != len(entry.Items) {
		t.Fatalf("loaded %d items, want %d", len(loaded.Items), len(entry.Items))
	}
	for index := range entry.Items {
		if !bytes.Equal(loaded.Items[index], entry.Items[index]) {
			t.Fatalf("item %d mismatch: %q vs %q", index, loaded.Items[index], entry.Items[index])
		}
	}
	if !loaded.Timestamp.Equal(entry.Timestamp) || loaded.Revision != entry.Revision ||
		loaded.Branch != entry.Branch || loaded.Deleted != entry.Deleted {
		t.Fatalf("metadata mismatch: loaded=%+v want=%+v", loaded, entry)
	}

	fileName, ok := antigravityReasoningReplayDiskFileName(model, session)
	if !ok {
		t.Fatal("valid key rejected by file-name derivation")
	}
	digest := sha256.Sum256([]byte(model + "\x00" + session))
	if want := fmt.Sprintf("%x.json", digest); fileName != want {
		t.Fatalf("file name = %q, want %q", fileName, want)
	}
	if strings.Contains(fileName, model) || strings.Contains(fileName, session) {
		t.Fatalf("raw key material leaked into file name %q", fileName)
	}
	entries, errList := os.ReadDir(root)
	if errList != nil {
		t.Fatal(errList)
	}
	if len(entries) != 1 {
		t.Fatalf("cache dir has %d entries, want 1", len(entries))
	}
	if entries[0].Name() != fileName {
		t.Fatalf("unexpected file %q, want %q", entries[0].Name(), fileName)
	}
	if strings.Contains(entries[0].Name(), "sessions") || strings.Contains(entries[0].Name(), "private") {
		t.Fatalf("raw session text leaked into stored file name %q", entries[0].Name())
	}
	if filepath.Join(root, entries[0].Name()) != filepath.Join(root, fileName) {
		t.Fatal("persisted file resolved outside the hashed location")
	}
}

func TestAntigravityReplayDiskKeyIsolation(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	if errSave := store.save("model-a", "session-1", antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
		t.Fatal(errSave)
	}
	for _, combo := range [][2]string{{"model-b", "session-1"}, {"model-a", "session-2"}, {"model-a", ""}, {"", "session-1"}} {
		if _, found := store.load(combo[0], combo[1], time.Now()); found {
			t.Fatalf("cross-match for %q/%q", combo[0], combo[1])
		}
	}
	loaded, found := store.load("model-a", "session-1", time.Now())
	if !found || len(loaded.Items) != 2 {
		t.Fatalf("own key lost: found=%v items=%d", found, len(loaded.Items))
	}
}

func TestAntigravityReplayDiskDeletedFlagRoundTrip(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	entry := antigravityReasoningReplayEntry{
		Timestamp: time.Now().Truncate(time.Millisecond),
		Revision:  7,
		Branch:    "tombstone-branch",
		Deleted:   true,
	}
	if errSave := store.save("model-a", "session-deleted", entry); errSave != nil {
		t.Fatal(errSave)
	}
	loaded, found := newAntigravityReasoningReplayDiskStore(root).load("model-a", "session-deleted", time.Now())
	if !found || !loaded.Deleted || len(loaded.Items) != 0 || loaded.Revision != 7 || loaded.Branch != "tombstone-branch" {
		t.Fatalf("deleted entry round trip = %+v found=%v", loaded, found)
	}
}

func TestAntigravityReplayDiskFileNameIsHashed(t *testing.T) {
	const model, session = "model-x", "session-y"
	fileName, ok := antigravityReasoningReplayDiskFileName(model, session)
	if !ok {
		t.Fatal("valid key rejected")
	}
	if !strings.HasSuffix(fileName, ".json") || len(fileName) != hex.EncodedLen(sha256.Size)+len(".json") {
		t.Fatalf("unexpected hashed name %q", fileName)
	}
	if _, errDecode := hex.DecodeString(fileName[:hex.EncodedLen(sha256.Size)]); errDecode != nil {
		t.Fatalf("file-name stem is not hex: %v", errDecode)
	}
	if strings.Contains(fileName, model) || strings.Contains(fileName, session) {
		t.Fatalf("raw key material leaked into file name %q", fileName)
	}
	for _, invalid := range [][2]string{{"", "session"}, {"model", "  "}, {"  ", "session"}} {
		if _, okInvalid := antigravityReasoningReplayDiskFileName(invalid[0], invalid[1]); okInvalid {
			t.Fatalf("invalid key %q/%q accepted", invalid[0], invalid[1])
		}
	}
}

func TestAntigravityReplayDiskRejectsCorruptJSON(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	if errSave := store.save("model", "session", antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
		t.Fatal(errSave)
	}
	path, _ := store.pathFor("model", "session")
	if errWrite := os.WriteFile(path, []byte(`{"items": [123`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("corrupt file accepted as a hit")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("corrupt file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskRejectsExpiredEntry(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	expired := antigravityReplayDiskTestEntry(time.Now().Add(-2*AntigravityReasoningReplayCacheTTL), false)
	if errSave := store.save("model", "session", expired); errSave != nil {
		t.Fatal(errSave)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("expired entry accepted")
	}
	path, _ := store.pathFor("model", "session")
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("expired file not removed: %v", errStat)
	}

	unexpired := antigravityReplayDiskTestEntry(time.Now().Add(-30*time.Minute), false)
	if errSave := store.save("model", "session", unexpired); errSave != nil {
		t.Fatal(errSave)
	}
	if _, found := store.load("model", "session", time.Now()); !found {
		t.Fatal("unexpired entry rejected")
	}
}

func TestAntigravityReplayDiskRejectsOversizedFile(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	if errSave := store.save("model", "session", antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
		t.Fatal(errSave)
	}
	path, _ := store.pathFor("model", "session")
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	padded := make([]byte, antigravityReasoningReplayCacheMaxSerializedBytes+1)
	copy(padded, data)
	if errWrite := os.WriteFile(path, padded, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("oversized file accepted")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("oversized file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskRejectsOversizedPayload(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	big := bytes.Repeat([]byte("a"), AntigravityReasoningReplayCacheMaxBytesPerEntry+1)
	raw, errMarshal := json.Marshal(antigravityReasoningReplayDiskFile{
		Items:     [][]byte{big},
		Timestamp: time.Now().Add(-time.Minute),
		Revision:  1,
		Branch:    "b",
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	path, _ := store.pathFor("model", "session")
	if errMkdir := os.MkdirAll(root, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("oversized payload accepted")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("oversized payload file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskRejectsTooManyItems(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	items := make([][]byte, AntigravityReasoningReplayCacheMaxItemsPerEntry+1)
	for index := range items {
		items[index] = []byte("x")
	}
	raw, errMarshal := json.Marshal(antigravityReasoningReplayDiskFile{
		Items:     items,
		Timestamp: time.Now().Add(-time.Minute),
		Revision:  1,
		Branch:    "b",
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	path, _ := store.pathFor("model", "session")
	if errMkdir := os.MkdirAll(root, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("over-item-limit file accepted")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("over-item-limit file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskRejectsUnsafeFileMode(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	if errSave := store.save("model", "session", antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
		t.Fatal(errSave)
	}
	path, _ := store.pathFor("model", "session")
	if errChmod := os.Chmod(path, 0o644); errChmod != nil {
		t.Fatal(errChmod)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("world-readable file accepted")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("unsafe file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskRejectsUnknownFields(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	timestamp := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	raw := []byte(`{"items":[["YQ=="]],"timestamp":"` + timestamp + `","revision":1,"branch":"b","deleted":false,"sneaky":1}`)
	path, _ := store.pathFor("model", "session")
	if errMkdir := os.MkdirAll(root, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("file with unknown fields accepted")
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("unknown-field file not removed: %v", errStat)
	}
}

func TestAntigravityReplayDiskFailedReplacementPreservesPriorFile(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	old := antigravityReplayDiskTestEntry(time.Now().Add(-time.Minute), false)
	old.Branch = "prior-branch"
	if errSave := store.save("model", "session", old); errSave != nil {
		t.Fatal(errSave)
	}
	if errChmod := os.Chmod(root, 0o500); errChmod != nil {
		t.Fatal(errChmod)
	}
	if errSave := store.save("model", "session", antigravityReplayDiskTestEntry(time.Now(), false)); errSave == nil {
		t.Fatal("save into read-only directory unexpectedly succeeded")
	}
	if errChmod := os.Chmod(root, 0o700); errChmod != nil {
		t.Fatal(errChmod)
	}
	loaded, found := store.load("model", "session", time.Now())
	if !found || loaded.Branch != "prior-branch" {
		t.Fatalf("failed replacement lost prior entry: found=%v branch=%q", found, loaded.Branch)
	}
	entries, errList := os.ReadDir(root)
	if errList != nil {
		t.Fatal(errList)
	}
	if len(entries) != 1 {
		t.Fatalf("temporary files left after failed save: %d files", len(entries))
	}
}

func TestAntigravityReplayDiskDeleteRemovesEntry(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	if errSave := store.save("model", "session", antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
		t.Fatal(errSave)
	}
	path, _ := store.pathFor("model", "session")
	if errDelete := store.delete("model", "session"); errDelete != nil {
		t.Fatal(errDelete)
	}
	if _, errStat := os.Lstat(path); !os.IsNotExist(errStat) {
		t.Fatalf("file still present after delete: %v", errStat)
	}
	if _, found := store.load("model", "session", time.Now()); found {
		t.Fatal("deleted entry still loads")
	}
	if errDelete := store.delete("model", "session"); errDelete != nil {
		t.Fatal("second delete failed")
	}
}

func TestAntigravityReplayDiskClearRemovesAllEntries(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	for index := 0; index < 3; index++ {
		if errSave := store.save("model", fmt.Sprintf("session-%d", index), antigravityReplayDiskTestEntry(time.Now(), false)); errSave != nil {
			t.Fatal(errSave)
		}
	}
	if errClear := store.clear(); errClear != nil {
		t.Fatal(errClear)
	}
	for index := 0; index < 3; index++ {
		if _, found := store.load("model", fmt.Sprintf("session-%d", index), time.Now()); found {
			t.Fatalf("session-%d survived clear", index)
		}
	}
	if _, errStat := os.Lstat(root); !os.IsNotExist(errStat) {
		t.Fatalf("cache directory survives clear: %v", errStat)
	}
	if errClear := store.clear(); errClear != nil {
		t.Fatal("clear on missing directory failed")
	}
}

func TestAntigravityReplayDiskSaveRejectsInvalidInput(t *testing.T) {
	root := t.TempDir()
	store := newAntigravityReasoningReplayDiskStore(root)
	tooMany := make([][]byte, AntigravityReasoningReplayCacheMaxItemsPerEntry+1)
	for index := range tooMany {
		tooMany[index] = []byte("x")
	}
	oversized := [][]byte{bytes.Repeat([]byte("a"), AntigravityReasoningReplayCacheMaxBytesPerEntry+1)}
	cases := []struct {
		model   string
		session string
		entry   antigravityReasoningReplayEntry
	}{
		{"", "session", antigravityReplayDiskTestEntry(time.Now(), false)},
		{"model", "   ", antigravityReplayDiskTestEntry(time.Now(), false)},
		{"model", "session", antigravityReasoningReplayEntry{Items: tooMany, Timestamp: time.Now()}},
		{"model", "session", antigravityReasoningReplayEntry{Items: oversized, Timestamp: time.Now()}},
	}
	for index, testCase := range cases {
		if errSave := store.save(testCase.model, testCase.session, testCase.entry); errSave == nil {
			t.Fatalf("case %d: save accepted invalid input", index)
		}
	}
	entries, errList := os.ReadDir(root)
	if errList != nil && !os.IsNotExist(errList) {
		t.Fatal(errList)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid saves wrote %d files", len(entries))
	}
}
