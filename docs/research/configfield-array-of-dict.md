# CLIProxyAPI `ConfigField`: array of dict / nested item schema

研究基线：CLIProxyAPI `main` commit `a7e3596b7e351d800e58ed29529fbca3d1c18737`（Go module `github.com/router-for-me/CLIProxyAPI/v7 v7.2.140`，2026-08-22）；管理前端 release `v1.22.6`，commit `6586f88858ca27e840bd8db2630dccd371a1cd4a`，asset `management.html` SHA-256 `e2643e0875e0024e5ff9ddf4569e4c58611ab0456aeb6fa6065ed3e6c2b721f4`。

## 结论

- `pluginapi.ConfigField` 支持 `Type: "array"` 和 `Type: "object"`，因此可以声明一个配置值是数组。
- `ConfigField` 没有 item type、item schema、properties、required、validation 或 nested fields 字段。它只能描述 `Name`、`Type`、可选 `EnumValues` 和 `Description`；所以 SDK contract **不能表达**“array of dict”的元素结构，也不能让管理 UI 自动生成嵌套表单。
- 管理 API 的数据 contract 本身接受任意递归 JSON/YAML 值。`PATCH` 的合并边界是插件配置对象的**顶层 key**；数组字段（包括数组中的 dict）作为一个值整体替换，不提供按数组元素或嵌套 key 的 patch 语义。
- 官方管理中心 v1.22.6 对 `array` 和 `object` 都渲染成 JSON `<textarea>`。保存时只验证顶层 JSON 类型：`array` 必须满足 `Array.isArray`，`object` 必须是非数组对象；不会检查数组元素类型或 dict 的字段。因此 `[{"provider":"codex","rpm":20}]` 可以保存，但在 UI 中只是手写 JSON，不是结构化 nested editor。

## Primary-source evidence

### 1. SDK `ConfigField` definition

`[sdk/pluginapi/types.go](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/sdk/pluginapi/types.go#L39-L69)` defines the seven scalar/container type values (`string`, `number`, `integer`, `boolean`, `enum`, `array`, `object`). The struct at lines 59-69 has only `Name`, `Type`, `EnumValues`, and `Description`; there is no schema-bearing field for array items or object properties.

The upstream example uses an array declaration for `tavily_api_keys` but supplies only a description, confirming the intended flat metadata surface: [`examples/plugin/claude-web-search-router/go/main.go#L266-L279`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/examples/plugin/claude-web-search-router/go/main.go#L266-L279).

### 2. Management API response and routes

The management list response exposes each field as exactly `name`, `type`, `enum_values`, and `description` in [`internal/api/handlers/management/plugins.go#L27-L56`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L27-L56). The conversion function copies only those four values and HTML-sanitizes them ([`plugins.go#L462-L472`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L462-L472)); any hypothetical nested schema cannot survive this response contract.

The native endpoints are registered as `GET/PUT/PATCH /v0/management/plugins/:id/config` ([`internal/api/server_management.go#L34-L41`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/server_management.go#L34-L41)). `GET` returns the preserved plugin config object as JSON ([`plugins.go#L158-L210`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L158-L210)). `PUT` accepts a JSON object and replaces the plugin config ([`plugins.go#L248-L274`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L248-L274)); `PATCH` iterates request keys, deletes nulls, and sets each value at that top-level key ([`plugins.go#L276-L310`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L276-L310)).

The JSON-to-YAML conversion is recursive: `[]any` becomes a YAML sequence and each item is recursively converted; `map[string]any` becomes a YAML mapping ([`plugins.go#L566-L616`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L566-L616)). The reverse conversion also recursively returns `[]any` and `map[string]any` ([`plugins.go#L618-L647`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/internal/api/handlers/management/plugins.go#L618-L647)). This is data preservation, not schema validation.

The upstream plugin README documents the same endpoint contract without a nested schema mechanism: [`examples/plugin/simple/README.md#L180-L193`](https://github.com/router-for-me/CLIProxyAPI/blob/a7e3596b7e351d800e58ed29529fbca3d1c18737/examples/plugin/simple/README.md#L180-L193).

### 3. Actual management UI rendering

The official management center source is [`Cli-Proxy-API-Management-Center`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/tree/6586f88858ca27e840bd8db2630dccd371a1cd4a), commit `6586f88858ca27e840bd8db2630dccd371a1cd4a` (release `v1.22.6`). Its typed frontend contract repeats the same flat four fields (`name`, `type`, `enumValues`, `description`) in [`src/types/plugin.ts#L1-L15`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/src/types/plugin.ts#L1-L15).

The plugin draft logic in [`src/features/plugins/pluginConfigDraft.ts`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/src/features/plugins/pluginConfigDraft.ts) does the following:

- serializes existing `array`/`object` values with `JSON.stringify(value, null, 2)`;
- parses JSON on save and checks only `Array.isArray` for `array` and `isRecord` for `object` ([lines 21-35 and 65-87](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/src/features/plugins/pluginConfigDraft.ts#L21-L87));
- passes the parsed value through unchanged for `array`/`object` ([lines 155-159](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/src/features/plugins/pluginConfigDraft.ts#L155-L159));
- has no item-schema/property traversal or nested controls.

The page renders both types as a single `<textarea>` with placeholder `[]` or `{}` ([`src/features/plugins/PluginsPage.tsx#L421-L436`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/src/features/plugins/PluginsPage.tsx#L421-L436)). The frontend test explicitly proves mixed array elements, including a dict, are accepted without coercion: [`tests/pluginConfigDraft.test.ts#L42-L54`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/blob/6586f88858ca27e840bd8db2630dccd371a1cd4a/tests/pluginConfigDraft.test.ts#L42-L54).

The release asset is also available as [`management.html`](https://github.com/router-for-me/Cli-Proxy-API-Management-Center/releases/download/v1.22.6/management.html); its SHA-256 is recorded above. The source tree is the preferred readable evidence.

## Practical implication for this plugin

To expose a provider policy such as an array of objects, a plugin can declare one field with `Type: ConfigFieldTypeArray` and describe the expected JSON shape in `Description`, for example:

```go
pluginapi.ConfigField{
    Name: "providers",
    Type: pluginapi.ConfigFieldTypeArray,
    Description: `JSON array of objects: [{"name":"codex","max_concurrency":2,"rpm":60}]`,
}
```

The UI will accept and persist that value, but validation of object keys/types must remain in the plugin's YAML/config parser. A `ConfigFieldTypeObject` field has the same limitation and is edited as raw JSON. If a friendlier UI is required, use flat fields or provide a plugin-owned management resource/API with its own schema-aware frontend; extending `ConfigField` would require a new SDK and management-center contract.
