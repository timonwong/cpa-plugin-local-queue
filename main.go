package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct { void* ptr; size_t len; } cliproxy_buffer;
typedef struct { uint32_t abi_version; void* host_ctx; void* call; void* free_buffer; } cliproxy_host_api;
typedef int (*cliproxy_plugin_call_fn)(char*, uint8_t*, size_t, cliproxy_buffer*);
typedef void (*cliproxy_plugin_free_fn)(void*, size_t);
typedef void (*cliproxy_plugin_shutdown_fn)(void);
typedef struct { uint32_t abi_version; cliproxy_plugin_call_fn call; cliproxy_plugin_free_fn free_buffer; cliproxy_plugin_shutdown_fn shutdown; } cliproxy_plugin_api;
extern int cliproxyPluginCall(char*, uint8_t*, size_t, cliproxy_buffer*);
extern void cliproxyPluginFree(void*, size_t);
extern void cliproxyPluginShutdown(void);
*/
import "C"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

var manager = newQueueManager()
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
func cliproxy_plugin_init(_ *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return 1
	}
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
func cliproxyPluginShutdown() { manager = newQueueManager() }

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
	policies, err := parseConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	return manager.configure(policies)
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
		},
	}, Capabilities: registrationCapabilities{Scheduler: true, RequestInterceptor: true, RequestLifecyclePlugin: true}}
}

func parseConfig(raw []byte) (map[string]providerPolicy, error) {
	var cfg pluginConfig
	if len(raw) > 0 {
		decoder := yaml.NewDecoder(bytes.NewReader(raw))
		decoder.KnownFields(true)
		if err := decoder.Decode(&cfg); err != nil {
			return nil, err
		}
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
			return nil, errors.New("enabled provider name must not be empty")
		}
		policies[provider] = policy
	}
	return policies, nil
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
