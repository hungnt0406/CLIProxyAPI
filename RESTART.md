# Restart CLIProxyAPI Server

## Quick Restart Commands

**Foreground (see output):**
```bash
lsof -ti:8317 | xargs kill -9 2>/dev/null || true; sleep 1; go run ./cmd/server
```

**Background (fire and forget):**
```bash
lsof -ti:8317 | xargs kill -9 2>/dev/null || true; sleep 1; go run ./cmd/server &
```

## What it does
1. Kills any existing process on port 8317
2. Waits 1 second for cleanup
3. Starts the server with `go run ./cmd/server`
