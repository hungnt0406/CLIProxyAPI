package cliproxy

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

// newReplayCacheServiceForTest builds a fully wired Service so the startup
// wiring under test runs against the real construction path used by Run.
func newReplayCacheServiceForTest(t *testing.T, cfg *config.Config) *Service {
	t.Helper()
	service, errBuild := NewBuilder().
		WithConfig(cfg).
		WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).
		Build()
	if errBuild != nil {
		t.Fatalf("Build() error = %v", errBuild)
	}
	return service
}

// isolateGlobalHomeClient clears any Home client registered by other tests in
// this package so replay cache writes deterministically take the local
// (non-Home) path, then restores the previous client afterwards.
func isolateGlobalHomeClient(t *testing.T) {
	t.Helper()
	previous := home.Current()
	home.ClearCurrent()
	t.Cleanup(func() {
		if previous != nil {
			home.SetCurrent(previous)
		} else {
			home.ClearCurrent()
		}
	})
}

func TestReplayCacheConfigRootFromAuthDir(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	authDir := t.TempDir()
	cfg := &config.Config{
		AuthDir: authDir,
		Host:    "127.0.0.1",
		Port:    0,
	}
	service := newReplayCacheServiceForTest(t, cfg)

	// This is the exact configuration step Run performs after AuthDir is
	// resolved and before request handling begins.
	service.configureAntigravityReplayCache(cfg)

	const model, session = "gemini-3.6-flash-high", "startup-wiring-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"startup-wiring-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	root := filepath.Join(authDir, "antigravity-replay")
	entries, errReadDir := os.ReadDir(root)
	if errReadDir != nil {
		t.Fatalf("replay cache root %s missing after startup wiring: %v", root, errReadDir)
	}
	if len(entries) != 1 {
		t.Fatalf("replay cache root has %d entries, want 1", len(entries))
	}
	authEntries, errListAuth := os.ReadDir(authDir)
	if errListAuth != nil {
		t.Fatal(errListAuth)
	}
	if len(authEntries) != 1 || authEntries[0].Name() != "antigravity-replay" {
		t.Fatalf("auth dir contains unexpected entries: %v", authEntries)
	}
}

func TestReplayCacheHomeConfigDisablesDiskRoot(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	authDir := t.TempDir()
	cfg := &config.Config{
		AuthDir: authDir,
		Host:    "127.0.0.1",
		Port:    0,
		Home:    internalconfig.HomeConfig{Enabled: true},
	}
	service := newReplayCacheServiceForTest(t, cfg)

	service.configureAntigravityReplayCache(cfg)

	const model, session = "gemini-3.6-flash-high", "home-mode-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"home-mode-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	if _, errStat := os.Lstat(filepath.Join(authDir, "antigravity-replay")); !os.IsNotExist(errStat) {
		t.Fatalf("Home mode selected a local disk root: %v", errStat)
	}
	authEntries, errListAuth := os.ReadDir(authDir)
	if errListAuth != nil {
		t.Fatal(errListAuth)
	}
	if len(authEntries) != 0 {
		t.Fatalf("Home mode wrote replay state into the auth dir: %v", authEntries)
	}
}

func TestReplayCacheConfigResolvesRelativeAuthDirAbsolute(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	baseDir := t.TempDir()
	t.Chdir(baseDir)

	cfg := &config.Config{
		AuthDir: "rel-auth-dir",
		Host:    "127.0.0.1",
		Port:    0,
	}
	service := newReplayCacheServiceForTest(t, cfg)

	// Startup sequence Run performs: resolve one absolute AuthDir, create it,
	// then wire the replay cache from the same resolved value.
	if errEnsure := service.ensureAuthDir(); errEnsure != nil {
		t.Fatalf("ensureAuthDir() error = %v", errEnsure)
	}
	service.configureAntigravityReplayCache(cfg)

	if !filepath.IsAbs(cfg.AuthDir) {
		t.Fatalf("AuthDir %q left relative after resolution", cfg.AuthDir)
	}
	wantRoot := filepath.Join(cfg.AuthDir, "antigravity-replay")

	// Moving the process CWD away must not relocate the replay root.
	otherDir := t.TempDir()
	t.Chdir(otherDir)

	const model, session = "gemini-3.6-flash-high", "relative-auth-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"relative-auth-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	entries, errReadDir := os.ReadDir(wantRoot)
	if errReadDir != nil {
		t.Fatalf("replay root %s missing after write: %v", wantRoot, errReadDir)
	}
	if len(entries) != 1 {
		t.Fatalf("replay root has %d entries, want 1", len(entries))
	}
	if _, errStat := os.Lstat(filepath.Join(otherDir, "rel-auth-dir")); !os.IsNotExist(errStat) {
		t.Fatalf("relative auth dir resolved through the process CWD: %v", errStat)
	}
}

func TestReplayCacheConfigResolvesTildeAuthDir(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	cfg := &config.Config{
		AuthDir: "~/tilde-auth-dir",
		Host:    "127.0.0.1",
		Port:    0,
	}
	service := newReplayCacheServiceForTest(t, cfg)

	if errEnsure := service.ensureAuthDir(); errEnsure != nil {
		t.Fatalf("ensureAuthDir() error = %v", errEnsure)
	}
	service.configureAntigravityReplayCache(cfg)

	wantAuthDir := filepath.Join(homeDir, "tilde-auth-dir")
	if cfg.AuthDir != wantAuthDir {
		t.Fatalf("AuthDir = %q, want %q", cfg.AuthDir, wantAuthDir)
	}
	const model, session = "gemini-3.6-flash-high", "tilde-auth-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"tilde-auth-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}
	entries, errReadDir := os.ReadDir(filepath.Join(wantAuthDir, "antigravity-replay"))
	if errReadDir != nil {
		t.Fatalf("replay root missing under tilde-resolved auth dir: %v", errReadDir)
	}
	if len(entries) != 1 {
		t.Fatalf("replay root has %d entries, want 1", len(entries))
	}
}

func TestReplayCacheConfigUpdateRewiresRoot(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	baseDir := t.TempDir()
	t.Chdir(baseDir)

	startupCfg := &config.Config{
		AuthDir: "startup-auth-dir",
		Host:    "127.0.0.1",
		Port:    0,
	}
	service := newReplayCacheServiceForTest(t, startupCfg)
	if errEnsure := service.ensureAuthDir(); errEnsure != nil {
		t.Fatalf("ensureAuthDir() error = %v", errEnsure)
	}
	service.configureAntigravityReplayCache(startupCfg)
	oldRoot := filepath.Join(startupCfg.AuthDir, "antigravity-replay")

	// Watcher-style runtime update points at a different relative auth dir.
	updatedCfg := &config.Config{
		AuthDir: "updated-auth-dir",
		Host:    "127.0.0.1",
		Port:    0,
	}
	service.applyWatcherConfigUpdate(updatedCfg)
	newRoot := filepath.Join(baseDir, "updated-auth-dir", "antigravity-replay")

	// The rewired root must stay anchored to the resolved dir when CWD moves.
	otherDir := t.TempDir()
	t.Chdir(otherDir)

	const model, session = "gemini-3.6-flash-high", "update-rewire-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"update-rewire-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	entries, errReadDir := os.ReadDir(newRoot)
	if errReadDir != nil {
		t.Fatalf("replay root not rewired after config update: %v", errReadDir)
	}
	if len(entries) != 1 {
		t.Fatalf("replay root has %d entries, want 1", len(entries))
	}
	if _, errStat := os.Lstat(oldRoot); !os.IsNotExist(errStat) {
		t.Fatalf("stale replay root still active after config update: %v", errStat)
	}
	if _, errStat := os.Lstat(filepath.Join(otherDir, "updated-auth-dir")); !os.IsNotExist(errStat) {
		t.Fatalf("updated auth dir resolved through the process CWD: %v", errStat)
	}
}

func TestReplayCacheConfigUpdateHomeDisablesRoot(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	authDir := t.TempDir()
	startupCfg := &config.Config{
		AuthDir: authDir,
		Host:    "127.0.0.1",
		Port:    0,
	}
	service := newReplayCacheServiceForTest(t, startupCfg)
	if errEnsure := service.ensureAuthDir(); errEnsure != nil {
		t.Fatalf("ensureAuthDir() error = %v", errEnsure)
	}
	service.configureAntigravityReplayCache(startupCfg)
	oldRoot := filepath.Join(authDir, "antigravity-replay")

	// Runtime update switches the service into Home mode.
	service.applyWatcherConfigUpdate(&config.Config{
		AuthDir: authDir,
		Host:    "127.0.0.1",
		Port:    0,
		Home:    internalconfig.HomeConfig{Enabled: true},
	})

	const model, session = "gemini-3.6-flash-high", "update-home-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"update-home-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	if _, errStat := os.Lstat(oldRoot); !os.IsNotExist(errStat) {
		t.Fatalf("Home mode update left local replay persistence active: %v", errStat)
	}
}

// TestReplayCacheConfigUpdateFailureKeepsRoot verifies that a runtime config
// update which fails partway through does not move the process-global replay
// root ahead of the runtime behavior that failed to apply.
func TestReplayCacheConfigUpdateFailureKeepsRoot(t *testing.T) {
	isolateGlobalHomeClient(t)
	cache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(func() {
		cache.ClearAntigravityReasoningReplayCache()
		cache.SetAntigravityReasoningReplayCacheRoot("")
	})

	authDirA := t.TempDir()
	startupCfg := &config.Config{AuthDir: authDirA, Host: "127.0.0.1", Port: 0}
	service := newReplayCacheServiceForTest(t, startupCfg)
	if errEnsure := service.ensureAuthDir(); errEnsure != nil {
		t.Fatalf("ensureAuthDir() error = %v", errEnsure)
	}
	service.configureAntigravityReplayCache(startupCfg)
	wantRoot := filepath.Join(authDirA, "antigravity-replay")

	// A later runtime step (pprof) fails; the update as a whole must fail.
	service.applyPprofConfigContextFn = func(context.Context, *config.Config) bool { return false }
	authDirB := t.TempDir()
	updatedCfg := &config.Config{AuthDir: authDirB, Host: "127.0.0.1", Port: 0}
	if applied := service.applyConfigUpdateWithAuthSynthesis(context.Background(), updatedCfg, false); applied {
		t.Fatal("runtime apply unexpectedly succeeded with failing pprof step")
	}

	const model, session = "gemini-3.6-flash-high", "failed-update-session"
	item := []byte(`{"type":"thought_signature","thoughtSignature":"failed-update-signature-1234567890"}`)
	if !cache.CacheAntigravityReasoningReplayItems(model, session, [][]byte{item}) {
		t.Fatal("replay cache write failed")
	}

	entries, errReadDir := os.ReadDir(wantRoot)
	if errReadDir != nil {
		t.Fatalf("replay root lost after failed runtime update: %v", errReadDir)
	}
	if len(entries) != 1 {
		t.Fatalf("replay root has %d entries, want 1", len(entries))
	}
	if _, errStat := os.Lstat(filepath.Join(authDirB, "antigravity-replay")); !os.IsNotExist(errStat) {
		t.Fatalf("replay root moved despite failed runtime update: %v", errStat)
	}
}
