package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/home"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v7/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v7/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	log "github.com/sirupsen/logrus"
)

// Run starts the service and blocks until the context is cancelled or the server stops.
// It initializes all components including authentication, file watching, HTTP server,
// and starts processing requests. The method blocks until the context is cancelled.
//
// Parameters:
//   - ctx: The context for controlling the service lifecycle
//
// Returns:
//   - error: An error if the service fails to start or run
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("cliproxy: service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, runCancel := context.WithCancel(ctx)
	s.homeMu.Lock()
	s.runCancel = runCancel
	s.homeMu.Unlock()
	defer func() {
		runCancel()
		s.homeMu.Lock()
		if s.runCancel != nil {
			s.runCancel = nil
		}
		s.homeMu.Unlock()
	}()

	usage.StartDefault(ctx)
	homeEnabled := s.cfg != nil && s.cfg.Home.Enabled
	if homeEnabled {
		forceHomeRuntimeConfig(s.cfg)
		redisqueue.SetUsageStatisticsEnabled(true)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	defer func() {
		if err := s.Shutdown(shutdownCtx); err != nil {
			log.Errorf("service shutdown returned error: %v", err)
		}
	}()

	if !homeEnabled {
		if errEnsureAuthDir := s.ensureAuthDir(); errEnsureAuthDir != nil {
			return errEnsureAuthDir
		}
	}

	s.configureAntigravityReplayCache(s.cfg)
	s.applyRetryConfig(s.cfg)
	s.configureCooldownStateStore(s.cfg)

	s.registerPluginAuthParser()
	if s.coreManager != nil && !homeEnabled {
		if errLoad := s.coreManager.Load(ctx); errLoad != nil {
			log.Warnf("failed to load auth store: %v", errLoad)
		}
		s.registerConfigAPIKeyAuths(coreauth.WithSkipPersist(ctx), s.cfg)
		if s.cfg.SaveCooldownStatus {
			if errRestoreCooldown := s.coreManager.RestoreCooldownStates(ctx); errRestoreCooldown != nil {
				log.Warnf("failed to restore cooldown state: %v", errRestoreCooldown)
			}
		}
	}

	if !homeEnabled {
		tokenResult, err := s.tokenProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if tokenResult == nil {
			tokenResult = &TokenClientResult{}
		}

		apiKeyResult, err := s.apiKeyProvider.Load(ctx, s.cfg)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		if apiKeyResult == nil {
			apiKeyResult = &APIKeyClientResult{}
		}
	}

	// legacy clients removed; no caches to refresh

	s.ensureWebsocketGateway()
	if homeEnabled {
		s.registerAvailableExecutors(ctx, executorRegistrationOptions{
			includeBaseline: true,
		})
		// Home mode does not expose in-process Redis RESP usage output; usage is forwarded to home instead.
		redisqueue.SetEnabled(true)
	}

	// handlers no longer depend on legacy clients; pass nil slice initially
	s.server = api.NewServer(s.cfg, s.coreManager, s.accessManager, s.configPath, s.serverOptions...)
	s.syncPluginRuntimeConfig(ctx)
	if homeEnabled {
		s.syncPluginModelRuntime(ctx)
	}

	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	if homeEnabled {
		s.startHomeSubscriber(ctx)
	}

	if s.server != nil && s.wsGateway != nil {
		s.server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		s.server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if errStop := s.wsGateway.Stop(ctx); errStop != nil {
					log.Warnf("failed to reset websocket connections after ws-auth change %t -> %t: %v", oldEnabled, newEnabled, errStop)
					return
				}
				log.Debugf("ws-auth enabled; existing websocket sessions terminated to enforce authentication")
				return
			}
			log.Debugf("ws-auth disabled; existing websocket sessions remain connected")
		})
	}

	if s.hooks.OnBeforeStart != nil {
		s.hooks.OnBeforeStart(s.cfg)
	}

	s.serverErr = make(chan error, 1)
	go func() {
		if errStart := s.server.Start(); errStart != nil {
			s.serverErr <- errStart
		} else {
			s.serverErr <- nil
		}
	}()

	time.Sleep(100 * time.Millisecond)
	fmt.Printf("API server started successfully on: %s:%d\n", s.cfg.Host, s.cfg.Port)

	s.applyPprofConfig(s.cfg)

	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}

	if !homeEnabled {
		var watcherWrapper *WatcherWrapper
		reloadCallback := func(newCfg *config.Config) { s.applyWatcherConfigUpdate(newCfg) }

		watcherWrapper, errCreate := s.watcherFactory(s.configPath, s.cfg.AuthDir, reloadCallback)
		if errCreate != nil {
			return fmt.Errorf("cliproxy: failed to create watcher: %w", errCreate)
		}
		s.watcher = watcherWrapper
		s.ensureAuthUpdateQueue(ctx)
		if s.authUpdates != nil {
			watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
		}
		watcherWrapper.SetConfig(s.cfg)
		s.registerPluginAuthParser()

		watcherCtx, watcherCancel := context.WithCancel(context.Background())
		s.watcherCancel = watcherCancel
		if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
			return fmt.Errorf("cliproxy: failed to start watcher: %w", errStart)
		}
		log.Info("file watcher started for config and auth directory changes")
		s.syncPluginModelRuntime(ctx)
	}

	s.registerModelRefreshCallback()

	// Prefer core auth manager auto refresh if available.
	if s.coreManager != nil && !homeEnabled {
		interval := 15 * time.Minute
		s.coreManager.StartAutoRefresh(context.Background(), interval)
		log.Infof("core auth auto-refresh started (interval=%s)", interval)
	}

	select {
	case <-ctx.Done():
		log.Debug("service context cancelled, shutting down...")
		return ctx.Err()
	case errServer := <-s.serverErr:
		return errServer
	}
}

// Shutdown gracefully stops background workers and the HTTP server.
// It ensures all resources are properly cleaned up and connections are closed.
// The shutdown is idempotent and can be called multiple times safely.
//
// Parameters:
//   - ctx: The context for controlling the shutdown timeout
//
// Returns:
//   - error: An error if shutdown fails
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	var shutdownErr error
	s.shutdownOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}

		s.homeLifecycleMu.Lock()
		if supervisor := s.homeSupervisor; supervisor != nil {
			s.homeConfigCommitMu.Lock()
			supervisor.cancel()
			s.homeConfigCommitMu.Unlock()
			<-supervisor.done
		}
		s.homeMu.Lock()
		homeCancel := s.homeCancel
		homeClient := s.homeClient
		homeRegistry := s.homeRegistry
		homeDispatchBundle := s.homeDispatchBundle
		homeForwarder := s.homeLogForwarder
		homeForwarderClient := s.homeLogForwarderClient
		s.homeGeneration++
		s.homeCancel = nil
		s.homeClient = nil
		s.homeRegistry = nil
		s.homeDispatchBundle = nil
		s.homeDrainBound = 0
		s.homeLogForwarder = nil
		s.homeLogForwarderClient = nil
		s.homeMu.Unlock()
		if s.coreManager != nil {
			s.coreManager.ClearHomeDispatchBundle(homeDispatchBundle)
		}
		home.ClearCurrentIf(homeClient)
		if homeCancel != nil {
			homeCancel()
		}
		if homeRegistry != nil {
			if errClose := homeRegistry.Close(); errClose != nil {
				log.WithError(errClose).Warn("failed to close Home execution registry during shutdown")
			}
		}
		if homeClient != nil {
			homeClient.Close()
		}
		if homeForwarder != nil {
			if homeForwarderClient == homeClient {
				homeForwarder.Deactivate(homeClient)
			}
			homeForwarder.Stop()
		}
		s.homeLifecycleMu.Unlock()

		// legacy refresh loop removed; only stopping core auth manager below

		if s.watcherCancel != nil {
			s.watcherCancel()
		}
		if s.coreManager != nil {
			s.coreManager.StopAutoRefresh()
		}
		if s.watcher != nil {
			if err := s.watcher.Stop(); err != nil {
				log.Errorf("failed to stop file watcher: %v", err)
				shutdownErr = err
			}
		}
		if s.wsGateway != nil {
			if err := s.wsGateway.Stop(ctx); err != nil {
				log.Errorf("failed to stop websocket gateway: %v", err)
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}
		if s.authQueueStop != nil {
			s.authQueueStop()
			s.authQueueStop = nil
		}

		if errShutdownPprof := s.shutdownPprof(ctx); errShutdownPprof != nil {
			log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
			if shutdownErr == nil {
				shutdownErr = errShutdownPprof
			}
		}

		// no legacy clients to persist

		if s.server != nil {
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			if err := s.server.Stop(shutdownCtx); err != nil {
				log.Errorf("error stopping API server: %v", err)
				if shutdownErr == nil {
					shutdownErr = err
				}
			}
		}

		if s.pluginHost != nil {
			sdktranslator.SetPluginHooks(nil)
			sdkAuth.RegisterPluginAuthParser(nil)
			if s.watcher != nil {
				s.watcher.SetPluginAuthParser(nil)
			}
			s.pluginHost.ApplyConfig(ctx, &config.Config{})
			s.pluginHost.RegisterModels(ctx, registry.GetGlobalRegistry())
			s.registerAvailableExecutors(ctx, executorRegistrationOptions{
				includePlugins: true,
			})
			s.pluginHost.RegisterFrontendAuthProviders()
			s.pluginHost.ShutdownAllContext(ctx)
			if s.accessManager != nil {
				s.accessManager.SetProviders(sdkaccess.RegisteredProviders())
			}
		}

		usage.StopDefault()
	})
	return shutdownErr
}

// resolveAuthDirAbsolute resolves cfg.AuthDir to one canonical absolute path
// shared by auth-dir creation and replay-cache wiring.
//
// [WHY]
// A relative `auth-dir` value would make both the auth directory and the
// replay root resolve through the process working directory, silently moving
// state when the CWD changes; every consumer must agree on one absolute path.
//
// [HOW]
// 1. Apply tilde expansion and the default directory via util.ResolveAuthDir.
// 2. Anchor the result against the current working directory.
//
// [RULES / NOTES]
//   - An empty AuthDir falls back to the default directory and stays absolute.
//   - Never returns the raw YAML value; callers failing here must not create
//     or persist anything under a guessed path.
//
// @param cfg the active service configuration.
// @return the absolute auth directory, or an error when it cannot be resolved.
func resolveAuthDirAbsolute(cfg *config.Config) (string, error) {
	if cfg == nil {
		return "", nil
	}
	resolved, errResolve := util.ResolveAuthDir(cfg.AuthDir)
	if errResolve != nil {
		return "", errResolve
	}
	return filepath.Abs(resolved)
}

// configureAntigravityReplayCache wires standalone replay persistence to the
// resolved authentication directory, or disables local persistence for Home
// mode.
//
// [WHY]
// Standalone deployments have no Home KV store; replay provenance must survive
// restarts under the resolved AuthDir, while Home mode must keep replay state
// in Home KV and never touch the local disk.
//
// [HOW]
//  1. Disable the local root when the configuration is nil or Home is enabled.
//  2. Resolve AuthDir to one absolute path (tilde expansion, default, and
//     CWD anchoring applied), never the raw `auth-dir` YAML value.
//  3. Point the replay cache at `<AuthDir>/antigravity-replay`.
//  4. Fall back to an empty root when resolution fails, keeping the cache
//     in-memory only.
//
// [RULES / NOTES]
//   - An empty root restores the original in-memory-only behavior.
//   - Called from Run after AuthDir is resolved and before request handling,
//     and from applyConfigRuntime only after a runtime update fully succeeds,
//     so a failed update never moves the root ahead of live runtime behavior.
//
// @param cfg the active service configuration.
func (s *Service) configureAntigravityReplayCache(cfg *config.Config) {
	if cfg == nil || cfg.Home.Enabled {
		cache.SetAntigravityReasoningReplayCacheRoot("")
		return
	}
	authDir, errResolve := resolveAuthDirAbsolute(cfg)
	if errResolve != nil {
		log.Warnf("failed to resolve auth directory for antigravity replay cache: %v", errResolve)
		cache.SetAntigravityReasoningReplayCacheRoot("")
		return
	}
	cache.SetAntigravityReasoningReplayCacheRoot(filepath.Join(authDir, "antigravity-replay"))
}

func (s *Service) ensureAuthDir() error {
	authDir, errResolve := resolveAuthDirAbsolute(s.cfg)
	if errResolve != nil {
		return fmt.Errorf("cliproxy: failed to resolve auth directory: %w", errResolve)
	}
	// Write the resolved absolute path back so auth-dir creation, the replay
	// cache root, and every later consumer agree on the same directory.
	s.cfg.AuthDir = authDir
	info, err := os.Stat(authDir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(authDir, 0o755); mkErr != nil {
				return fmt.Errorf("cliproxy: failed to create auth directory %s: %w", authDir, mkErr)
			}
			log.Infof("created missing auth directory: %s", authDir)
			return nil
		}
		return fmt.Errorf("cliproxy: error checking auth directory %s: %w", authDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cliproxy: auth path exists but is not a directory: %s", authDir)
	}
	return nil
}
