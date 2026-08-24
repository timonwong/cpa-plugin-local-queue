package main

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestPluginRegistrationExposesFlatProviderConfigFields(t *testing.T) {
	fields := pluginRegistration().Metadata.ConfigFields
	got := make([]string, 0, len(fields))
	for _, field := range fields {
		got = append(got, field.Name)
	}
	if want := []string{"max_concurrency", "rpm", "max_queue", "max_wait", "enabled_providers", "log_level"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("config fields: got %v, want %v", got, want)
	}
}

func TestParseConfigUsesSharedPolicyForEnabledProviders(t *testing.T) {
	cfg, err := parseConfig([]byte(`max_concurrency: 5
rpm: 20
max_queue: 100
max_wait: 5m
enabled_providers: [codex, claude]
log_level: debug
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.policies["codex"]; got.MaxConcurrency != 5 || got.RPM != 20 || got.MaxQueue != 100 || got.MaxWaitText != "5m" {
		t.Fatalf("codex policy: %#v", got)
	}
	if got := cfg.policies["claude"]; got != cfg.policies["codex"] {
		t.Fatalf("claude policy: %#v", got)
	}
	if cfg.logLevel != logLevelDebug {
		t.Fatalf("log level: got %s, want debug", cfg.logLevel)
	}
}

func TestParseConfigRejectsLegacyProvidersField(t *testing.T) {
	_, err := parseConfig([]byte("providers:\n  codex: {}\n"))
	if err == nil || !strings.Contains(err.Error(), "field providers not found") {
		t.Fatalf("got %v, want legacy providers field error", err)
	}
}

func TestParseConfigIgnoresHostPluginMetadata(t *testing.T) {
	cfg, err := parseConfig([]byte(`enabled: true
max_concurrency: 5
rpm: 20
max_queue: 100
max_wait: 5m
enabled_providers: [codex]
store:
  id: local-queue
  version: 0.2.0
priority: 10
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.policies["codex"]; !ok {
		t.Fatalf("codex policy missing: %#v", cfg.policies)
	}
}

func TestParseConfigDefaultsLogLevelToInfo(t *testing.T) {
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.logLevel != logLevelInfo {
		t.Fatalf("log level: got %s, want info", cfg.logLevel)
	}
}

func TestParseConfigRejectsUnknownLogLevel(t *testing.T) {
	_, err := parseConfig([]byte("log_level: verbose\n"))
	if err == nil || !strings.Contains(err.Error(), "log_level must be one of") {
		t.Fatalf("got %v, want invalid log level error", err)
	}
}

func TestPluginRegisterReturnsMetadataForLegacyConfig(t *testing.T) {
	manager = newQueueManager()
	configRaw, err := json.Marshal(lifecycleRequest{
		SchemaVersion: pluginabi.SchemaVersion,
		ConfigYAML:    []byte("providers:\n  codex: {}\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := handleMethod(pluginabi.MethodPluginRegister, configRaw)
	if err != nil {
		t.Fatal(err)
	}
	var response envelope
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK {
		t.Fatalf("registration failed: %s", response.Error.Message)
	}
	var registered registration
	if err := json.Unmarshal(response.Result, &registered); err != nil {
		t.Fatal(err)
	}
	if len(registered.Metadata.ConfigFields) == 0 {
		t.Fatal("registration returned no config fields")
	}
}

func TestPluginRPCAdmitsSelectedCredentialAndReleasesOnCompletion(t *testing.T) {
	manager = newQueueManager()
	configRaw, err := json.Marshal(lifecycleRequest{
		SchemaVersion: pluginabi.SchemaVersion,
		ConfigYAML:    []byte("max_concurrency: 1\nrpm: 10\nmax_queue: 0\nmax_wait: 1s\nenabled_providers: [codex]\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handleMethod(pluginabi.MethodPluginReconfigure, configRaw); err != nil {
		t.Fatal(err)
	}
	schedulerRaw, err := json.Marshal(pluginapi.SchedulerPickRequest{Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "auth-a", Provider: "codex"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handleMethod(pluginabi.MethodSchedulerPick, schedulerRaw); err != nil {
		t.Fatal(err)
	}

	first := interceptResult(t, "request-a", "auth-a")
	if first.Terminate {
		t.Fatalf("first request terminated: %#v", first)
	}
	second := interceptResult(t, "request-b", "auth-a")
	if !second.Terminate || second.StatusCode != 429 {
		t.Fatalf("second response: %#v", second)
	}

	completionRaw, err := json.Marshal(pluginapi.RequestCompletion{RequestID: "request-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handleMethod(pluginabi.MethodRequestComplete, completionRaw); err != nil {
		t.Fatal(err)
	}
	third := interceptResult(t, "request-c", "auth-a")
	if third.Terminate {
		t.Fatalf("third request terminated after completion: %#v", third)
	}
}

func TestPluginRPCBypassesUnconfiguredCredential(t *testing.T) {
	manager = newQueueManager()
	configRaw, err := json.Marshal(lifecycleRequest{
		SchemaVersion: pluginabi.SchemaVersion,
		ConfigYAML:    []byte("max_concurrency: 1\nrpm: 1\nmax_queue: 0\nmax_wait: 1s\nenabled_providers: [codex]\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handleMethod(pluginabi.MethodPluginReconfigure, configRaw); err != nil {
		t.Fatal(err)
	}
	schedulerRaw, err := json.Marshal(pluginapi.SchedulerPickRequest{Candidates: []pluginapi.SchedulerAuthCandidate{{ID: "auth-a", Provider: "claude"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handleMethod(pluginabi.MethodSchedulerPick, schedulerRaw); err != nil {
		t.Fatal(err)
	}
	if result := interceptResult(t, "request-a", "auth-a"); result.Terminate {
		t.Fatalf("unconfigured provider was limited: %#v", result)
	}
}

func TestCompletionReleasesEveryCredentialUsedByOneRequest(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{
		"codex": {MaxConcurrency: 1, RPM: 10, MaxQueue: 0, MaxWait: time.Second},
	}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"auth-a": "codex", "auth-b": "codex"})
	if err := m.acquire(context.Background(), "request-a", "auth-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(context.Background(), "request-a", "auth-b"); err != nil {
		t.Fatal(err)
	}
	m.release("request-a")
	if err := m.acquire(context.Background(), "request-b", "auth-a"); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(context.Background(), "request-c", "auth-b"); err != nil {
		t.Fatal(err)
	}
}

func interceptResult(t *testing.T, requestID, authID string) pluginapi.RequestInterceptResponse {
	t.Helper()
	raw, err := json.Marshal(pluginapi.RequestInterceptRequest{RequestID: requestID, Metadata: map[string]any{"selected_auth_id": authID}})
	if err != nil {
		t.Fatal(err)
	}
	responseRaw, err := handleMethod(pluginabi.MethodRequestInterceptAfter, raw)
	if err != nil {
		t.Fatal(err)
	}
	var envelope envelope
	if err := json.Unmarshal(responseRaw, &envelope); err != nil {
		t.Fatal(err)
	}
	var response pluginapi.RequestInterceptResponse
	if err := json.Unmarshal(envelope.Result, &response); err != nil {
		t.Fatal(err)
	}
	return response
}
