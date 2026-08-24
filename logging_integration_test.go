//go:build cgo && integration && (darwin || linux || freebsd)

package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginhost"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func TestPluginEmitsLogsThroughCPAHostSDK(t *testing.T) {
	tempDir := t.TempDir()
	platformDir := filepath.Join(tempDir, runtime.GOOS, runtime.GOARCH)
	if err := os.MkdirAll(platformDir, 0o755); err != nil {
		t.Fatal(err)
	}
	ext := "so"
	if runtime.GOOS == "darwin" {
		ext = "dylib"
	}
	artifact := filepath.Join(platformDir, "local-queue."+ext)
	build := exec.Command("go", "build", "-buildmode=c-shared", "-o", artifact, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build plugin: %v\n%s", err, output)
	}

	var raw yaml.Node
	if err := yaml.Unmarshal([]byte(`max_concurrency: 1
rpm: 10
max_queue: 1
max_wait: 1s
enabled_providers: [codex]
log_level: debug
`), &raw); err != nil {
		t.Fatal(err)
	}
	enabled := true

	var output bytes.Buffer
	standard := log.StandardLogger()
	originalOutput := standard.Out
	originalFormatter := standard.Formatter
	originalLevel := standard.Level
	log.SetOutput(&output)
	log.SetFormatter(&log.TextFormatter{DisableColors: true, DisableTimestamp: true})
	log.SetLevel(log.DebugLevel)
	t.Cleanup(func() {
		log.SetOutput(originalOutput)
		log.SetFormatter(originalFormatter)
		log.SetLevel(originalLevel)
	})

	host := pluginhost.New()
	t.Cleanup(host.ShutdownAll)
	host.ApplyConfig(context.Background(), pluginhost.RuntimeConfig{
		Enabled: true,
		Dir:     tempDir,
		Configs: map[string]pluginhost.PluginInstanceConfig{
			"local-queue": {Enabled: &enabled, Raw: raw},
		},
	})

	registered := host.RegisteredPlugins()
	if len(registered) != 1 || registered[0].ID != "local-queue" {
		t.Fatalf("registered plugins: %#v", registered)
	}
	logs := output.String()
	for _, expected := range []string{
		"local queue configuration applied",
		"log_level=debug",
		"providers=codex",
		"plugin_id=local-queue",
	} {
		if !strings.Contains(logs, expected) {
			t.Fatalf("CPA host log missing %q:\n%s", expected, logs)
		}
	}
	for _, unwanted := range []string{"enabled_providers=", "state="} {
		if strings.Contains(logs, unwanted) {
			t.Fatalf("CPA host log contains redundant field %q:\n%s", unwanted, logs)
		}
	}
}
