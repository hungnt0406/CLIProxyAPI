package cliproxy

import (
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
