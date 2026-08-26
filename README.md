# CPA Local Queue Plugin

This Go plugin adds credential-level FIFO admission to CLIProxyAPI with one shared policy and an explicit list of enabled providers.

Only providers present in the plugin configuration are limited. For every selected credential of a configured provider, the plugin maintains an independent queue, concurrency counter, and one-minute request window. Credentials belonging to providers that are not configured pass through unchanged.

```yaml
plugins:
  configs:
    local-queue:
      max_concurrency: 5
      rpm: 20
      max_queue: 100
      max_wait: 5m
      enabled_providers:
        - codex
        - claude
      log_level: info
```

The plugin learns `auth_id -> provider` from `scheduler.pick`, admits the selected credential in `request.intercept_after`, and releases it from `request.complete`. It returns HTTP 429 with `Retry-After: 1` when a queue is full or the wait deadline expires.

The plugin does not modify CPA's upstream retry behavior. In particular, the current ABI does not provide a callback that can resubmit an upstream 429 to the original credential queue.

## GUI configuration

The management UI exposes one shared policy and one provider list:

- `max_concurrency`
- `rpm`
- `max_queue`
- `max_wait`
- `enabled_providers`
- `log_level` (`error`, `warn`, `info`, `debug`, or `trace`; defaults to `info`)

`enabled_providers` is a JSON array, for example:

```json
["codex", "claude"]
```

All enabled providers use the same policy values. Their runtime queues and rate windows remain independent per selected credential.

## Logging

The plugin emits structured logs through CPA's `host.log` callback, so they use the host's normal log output and log streaming path. `info` reports configuration changes, `warn` reports queue rejections, `debug` reports queue admission and release, and `trace` reports scheduler observations and bypasses caused by an unknown mapping, an unconfigured provider, or reconfiguration. Each event keeps a short message and only the context needed to understand it: request, credential, provider, and queue counters where applicable. The host supplies the plugin identity; the plugin does not duplicate it. Logs never include request bodies or credential contents.

## Plugin store

The plugin store registry is maintained separately from the source tree and contains only this plugin:

```text
https://raw.githubusercontent.com/timonwong/cpa-plugin-local-queue/plugin-store-release/registry.json
```

Configure CPA to use the store with a GitHub token that can read this repository:

```yaml
plugins:
  store-sources:
    - "https://raw.githubusercontent.com/timonwong/cpa-plugin-local-queue/plugin-store-release/registry.json"
  store-auth:
    - match: "https://raw.githubusercontent.com/timonwong/cpa-plugin-local-queue/"
      apply-to: ["registry", "artifact"]
      type: github-token
      token-env: "CLIPROXY_PLUGIN_STORE_TOKEN"
```

The release workflow updates the registry branch and creates a versioned store snapshot tag such as `plugin-store-release/v0.1.0`.

## Operational limits

Admission is a blocking call across the C ABI, so every waiting request holds one host OS thread for as long as it stays queued. Size the configuration accordingly: `credentials x max_queue` is the worst-case thread budget the host must be able to absorb.

A full queue or an expired `max_wait` returns HTTP 429 to the client and terminates the request. CPA does not retry it against another credential, so `max_queue` and `max_wait` decide how much load is absorbed rather than rejected.

## Build

```bash
go test ./...
go test -race ./...
go build -buildmode=c-shared -o local-queue.so .
```

The CPA SDK integration test builds the dynamic library, loads it through `sdk/pluginhost`, and verifies that `host.log` reaches the host logger:

```bash
go test -tags=integration -run '^TestPluginEmitsLogsThroughCPAHostSDK$' ./...
```

Install the resulting dynamic library using the CPA plugin directory for the target platform. The artifact filename must match the plugin ID configured in CPA.
