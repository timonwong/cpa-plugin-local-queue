package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

func TestPluginRPCAdmitsSelectedCredentialAndReleasesOnCompletion(t *testing.T) {
	manager = newQueueManager()
	configRaw, err := json.Marshal(lifecycleRequest{
		SchemaVersion: pluginabi.SchemaVersion,
		ConfigYAML:    []byte("providers:\n  codex:\n    max_concurrency: 1\n    rpm: 10\n    max_queue: 0\n    max_wait: 1s\n"),
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
		ConfigYAML:    []byte("providers:\n  codex:\n    max_concurrency: 1\n    rpm: 1\n    max_queue: 0\n    max_wait: 1s\n"),
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
