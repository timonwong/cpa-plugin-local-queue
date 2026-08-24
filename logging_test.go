package main

import (
	"reflect"
	"testing"
)

func TestPluginLoggerFiltersEventsByConfiguredLevel(t *testing.T) {
	var got []string
	l := newPluginLogger(func(event logEvent) { got = append(got, event.Level) })
	l.setLevel(logLevelWarn)

	l.log(logLevelError, "error", nil)
	l.log(logLevelWarn, "warn", nil)
	l.log(logLevelInfo, "info", nil)

	if want := []string{"error", "warn"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("levels: got %v, want %v", got, want)
	}
}

func TestPluginLoggerPreservesStructuredFieldsWithoutHostOwnedIdentity(t *testing.T) {
	var got logEvent
	l := newPluginLogger(func(event logEvent) { got = event })
	l.log(logLevelInfo, "configured", map[string]any{"enabled_providers": 2})

	if got.Message != "configured" || got.Fields["enabled_providers"] != 2 {
		t.Fatalf("event: %#v", got)
	}
	if _, ok := got.Fields["plugin_id"]; ok {
		t.Fatalf("event contains host-owned plugin_id: %#v", got)
	}
}
