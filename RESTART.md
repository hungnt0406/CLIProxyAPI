# Restart CLIProxyAPI Server

## Quick Restart Commands

**Foreground (see output):**
```bash
lsof -ti:8317 | xargs kill -9 2>/dev/null || true; sleep 1; go run ./cmd/server
```

**Background (fire and forget, no terminal logs):**
```bash
lsof -ti:8317 | xargs kill -9 2>/dev/null || true; sleep 1; nohup go run ./cmd/server > /tmp/cliproxyapi.log 2>&1 & disown
```

## What it does
1. Kills any existing process on port 8317
2. Waits 1 second for cleanup
3. Starts the server with `go run ./cmd/server`, detached from the terminal via `nohup`/`disown` so it keeps running after the session ends
4. Output is redirected to `/tmp/cliproxyapi.log` (view with `tail -f /tmp/cliproxyapi.log` if needed)
