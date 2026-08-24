# CPA Local Queue Plugin

This Go plugin adds credential-level FIFO admission to CLIProxyAPI while keeping configuration at the provider level.

Only providers present in the plugin configuration are limited. For every selected credential of a configured provider, the plugin maintains an independent queue, concurrency counter, and one-minute request window. Credentials belonging to providers that are not configured pass through unchanged.

```yaml
plugins:
  configs:
    local-queue:
      providers:
        codex:
          max_concurrency: 5
          rpm: 20
          max_queue: 100
          max_wait: 5m
        claude:
          max_concurrency: 3
          rpm: 10
          max_queue: 50
          max_wait: 3m
```

The plugin learns `auth_id -> provider` from `scheduler.pick`, admits the selected credential in `request.intercept_after`, and releases it from `request.complete`. It returns HTTP 429 with `Retry-After: 1` when a queue is full or the wait deadline expires.

The plugin does not modify CPA's upstream retry behavior. In particular, the current ABI does not provide a callback that can resubmit an upstream 429 to the original credential queue.

## Build

```bash
go test ./...
go test -race ./...
go build -buildmode=c-shared -o local-queue.so .
```

Install the resulting dynamic library using the CPA plugin directory for the target platform. The artifact filename must match the plugin ID configured in CPA.
