# Local Queue Plugin

## Domain

- A **provider** is CPA's provider key, such as `codex` or `claude`.
- A **credential** is one concrete CPA auth record selected for a request.
- Configuration has one shared queue policy and an explicit `enabled_providers` list; runtime state is keyed by selected credential `auth_id`.
- A credential whose provider is absent from `enabled_providers` bypasses this plugin.

## Admission Contract

Each configured credential owns an independent FIFO queue, in-flight counter, and one-minute RPM window. `max_queue` bounds waiting requests and `max_wait` bounds admission time. Completion releases an admitted request exactly once.

The plugin learns candidate `auth_id -> provider` mappings from `scheduler.pick`, admits the selected credential in `request.intercept_after`, and releases it from `request.complete`. It does not alter CPA's upstream retry/cooldown behavior because the current ABI has no resubmission callback.
