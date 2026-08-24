package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef int (*cliproxy_host_call_fn)(void*, const char*, const uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_host_free_fn)(void*, size_t);
typedef struct { uint32_t abi_version; void* host_ctx; cliproxy_host_call_fn call; cliproxy_host_free_fn free_buffer; } cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct { uint32_t abi_version; cliproxy_plugin_call_fn call; cliproxy_plugin_free_fn free_buffer; cliproxy_plugin_shutdown_fn shutdown; } cliproxy_plugin_api;
static const cliproxy_host_api* stored_host_api = NULL;
static void cliproxy_store_host_api(const cliproxy_host_api* host) { stored_host_api = host; }
static int cliproxy_host_log(const uint8_t* request, size_t request_len) {
	if (stored_host_api == NULL || stored_host_api->call == NULL) {
		return 1;
	}
	cliproxy_buffer response = {0};
	int rc = stored_host_api->call(stored_host_api->host_ctx, "host.log", request, request_len, &response);
	if (response.ptr != NULL && stored_host_api->free_buffer != NULL) {
		stored_host_api->free_buffer(response.ptr, response.len);
	}
	return rc;
}
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

var manager = newQueueManager()
var logger = newPluginLogger(writeHostLog)
var pluginVersion = "0.1.0"

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}
type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type pluginConfig struct {
	MaxConcurrency   int      `yaml:"max_concurrency"`
	RPM              int      `yaml:"rpm"`
	MaxQueue         int      `yaml:"max_queue"`
	MaxWaitText      string   `yaml:"max_wait"`
	EnabledProviders []string `yaml:"enabled_providers"`
	LogLevel         string   `yaml:"log_level"`
}

type registration struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Metadata      pluginapi.Metadata       `json:"metadata"`
	Capabilities  registrationCapabilities `json:"capabilities"`
}
type registrationCapabilities struct {
	Scheduler              bool `json:"scheduler"`
	RequestInterceptor     bool `json:"request_interceptor"`
	RequestLifecyclePlugin bool `json:"request_lifecycle_plugin"`
}

func main() {}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
	C.cliproxy_store_host_api(host)
	plugin.abi_version = C.uint32_t(pluginabi.ABIVersion)
	plugin.call = C.cliproxy_plugin_call_fn(C.cliproxyPluginCall)
	plugin.free_buffer = C.cliproxy_plugin_free_fn(C.cliproxyPluginFree)
	plugin.shutdown = C.cliproxy_plugin_shutdown_fn(C.cliproxyPluginShutdown)
	return 0
}

//export cliproxyPluginCall
func cliproxyPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response != nil {
		response.ptr = nil
		response.len = 0
	}
	if method == nil {
		writeResponse(response, errorEnvelope("invalid_method", "method is required"))
		return 1
	}
	var raw []byte
	if request != nil && requestLen > 0 {
		raw = C.GoBytes(unsafe.Pointer(request), C.int(requestLen))
	}
	out, err := handleMethod(C.GoString(method), raw)
	if err != nil {
		logger.log(logLevelError, "plugin method failed", map[string]any{"method": C.GoString(method), "error": err.Error()})
		writeResponse(response, errorEnvelope("plugin_error", err.Error()))
		return 1
	}
	writeResponse(response, out)
	return 0
}

//export cliproxyPluginFree
func cliproxyPluginFree(ptr unsafe.Pointer, _ C.size_t) {
	if ptr != nil {
		C.free(ptr)
	}
}

//export cliproxyPluginShutdown
func cliproxyPluginShutdown() {
	manager = newQueueManager()
	logger.setLevel(logLevelInfo)
	C.cliproxy_store_host_api(nil)
}

func handleMethod(method string, raw []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister:
		// Registration metadata must remain available when an existing config
		// needs migration; otherwise the host hides ConfigFields behind a
		// generic invalid-plugin result.
		if err := configure(raw); err != nil && !isLegacyConfigError(err) {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodPluginReconfigure:
		if err := configure(raw); err != nil {
			return nil, err
		}
		return okEnvelope(pluginRegistration())
	case pluginabi.MethodSchedulerPick:
		return schedulerPick(raw)
	case pluginabi.MethodRequestInterceptBefore:
		return passThrough(raw)
	case pluginabi.MethodRequestInterceptAfter:
		return interceptAfter(raw)
	case pluginabi.MethodRequestComplete:
		return complete(raw)
	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func isLegacyConfigError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "field providers not found")
}

func configure(raw []byte) error {
	var req lifecycleRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
	}
	if req.SchemaVersion < 2 {
		return fmt.Errorf("request lifecycle plugin requires host schema version 2 or newer")
	}
	cfg, err := parseConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	if err := manager.configure(cfg.policies); err != nil {
		return err
	}
	providers := make([]string, 0, len(cfg.policies))
	for provider := range cfg.policies {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	logger.setLevel(cfg.logLevel)
	logger.log(logLevelInfo, "local queue configuration applied", map[string]any{
		"providers": strings.Join(providers, ", "),
		"log_level": cfg.logLevel.String(),
	})
	return nil
}

func pluginRegistration() registration {
	return registration{SchemaVersion: pluginabi.SchemaVersion, Metadata: pluginapi.Metadata{
		Name: "local-queue", Version: pluginVersion, Author: "timonwong", GitHubRepository: "https://github.com/timonwong/cpa-plugin-local-queue",
		ConfigFields: []pluginapi.ConfigField{
			{Name: "max_concurrency", Type: pluginapi.ConfigFieldTypeInteger, Description: "Shared maximum concurrent requests for each enabled provider."},
			{Name: "rpm", Type: pluginapi.ConfigFieldTypeInteger, Description: "Shared maximum requests per minute for each enabled provider."},
			{Name: "max_queue", Type: pluginapi.ConfigFieldTypeInteger, Description: "Shared maximum waiting requests for each enabled provider."},
			{Name: "max_wait", Type: pluginapi.ConfigFieldTypeString, Description: "Shared maximum queue wait duration, such as 30s or 5m."},
			{Name: "enabled_providers", Type: pluginapi.ConfigFieldTypeArray, Description: "JSON array of provider names to enable."},
			{Name: "log_level", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"error", "warn", "info", "debug", "trace"}, Description: "Minimum plugin log level emitted through the CPA host logger."},
		},
	}, Capabilities: registrationCapabilities{Scheduler: true, RequestInterceptor: true, RequestLifecyclePlugin: true}}
}

type parsedConfig struct {
	policies map[string]providerPolicy
	logLevel logLevel
}

func parseConfig(raw []byte) (parsedConfig, error) {
	var cfg pluginConfig
	if len(raw) > 0 {
		var fields map[string]yaml.Node
		if err := yaml.Unmarshal(raw, &fields); err != nil {
			return parsedConfig{}, err
		}
		if _, legacy := fields["providers"]; legacy {
			return parsedConfig{}, errors.New("field providers not found in type main.pluginConfig")
		}
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return parsedConfig{}, err
		}
	}
	level, err := parseLogLevel(cfg.LogLevel)
	if err != nil {
		return parsedConfig{}, err
	}
	policy := providerPolicy{
		MaxConcurrency: cfg.MaxConcurrency,
		RPM:            cfg.RPM,
		MaxQueue:       cfg.MaxQueue,
		MaxWaitText:    cfg.MaxWaitText,
	}
	policies := make(map[string]providerPolicy, len(cfg.EnabledProviders))
	for _, provider := range cfg.EnabledProviders {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider == "" {
			return parsedConfig{}, errors.New("enabled provider name must not be empty")
		}
		policies[provider] = policy
	}
	return parsedConfig{policies: policies, logLevel: level}, nil
}

func schedulerPick(raw []byte) ([]byte, error) {
	// scheduler.pick has candidate/provider data but no request ID; admission waits after auth selection.
	var req pluginapi.SchedulerPickRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	providerByAuth := make(map[string]string, len(req.Candidates))
	for _, candidate := range req.Candidates {
		if strings.TrimSpace(candidate.ID) != "" {
			providerByAuth[candidate.ID] = candidate.Provider
		}
	}
	manager.rememberCandidates(providerByAuth)
	logger.log(logLevelTrace, "scheduler candidates observed", map[string]any{
		"candidate_count": len(providerByAuth),
	})
	return okEnvelope(pluginapi.SchedulerPickResponse{Handled: false})
}

func interceptAfter(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.RequestID) == "" {
		return nil, fmt.Errorf("request ID is required")
	}
	authID := metadataString(req.Metadata, "selected_auth_id", "SelectedAuthMetadataKey")
	if authID != "" {
		// The C ABI carries no cancellation context, so max_wait bounds this synchronous admission.
		if err := manager.acquire(context.Background(), req.RequestID, authID); err != nil {
			return rejected(http.StatusTooManyRequests, err.Error())
		}
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func complete(raw []byte) ([]byte, error) {
	var req pluginapi.RequestCompletion
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	manager.release(req.RequestID)
	return okEnvelope(struct{}{})
}

func passThrough(raw []byte) ([]byte, error) {
	var req pluginapi.RequestInterceptRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return nil, err
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Headers: req.Headers, Body: req.Body})
}

func metadataString(metadata map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func rejected(status int, message string) ([]byte, error) {
	body, err := json.Marshal(map[string]any{"error": map[string]any{"type": "plugin_request_rejected", "message": message}})
	if err != nil {
		return nil, err
	}
	return okEnvelope(pluginapi.RequestInterceptResponse{Terminate: true, StatusCode: status, ResponseHeaders: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"1"}}, ResponseBody: body})
}
func okEnvelope(value any) ([]byte, error) {
	result, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{OK: true, Result: result})
}
func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	return raw
}
func writeResponse(response *C.cliproxy_buffer, raw []byte) {
	if response == nil || len(raw) == 0 {
		return
	}
	ptr := C.CBytes(raw)
	if ptr == nil {
		return
	}
	response.ptr = ptr
	response.len = C.size_t(len(raw))
}

func writeHostLog(event logEvent) {
	raw, err := json.Marshal(event)
	if err != nil || len(raw) == 0 {
		return
	}
	request := C.CBytes(raw)
	if request == nil {
		return
	}
	defer C.free(request)
	C.cliproxy_host_log((*C.uint8_t)(request), C.size_t(len(raw)))
}
