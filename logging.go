package main

import (
	"fmt"
	"strings"
	"sync"
)

type logLevel uint8

const (
	logLevelError logLevel = iota
	logLevelWarn
	logLevelInfo
	logLevelDebug
	logLevelTrace
)

func parseLogLevel(value string) (logLevel, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return logLevelInfo, nil
	case "error":
		return logLevelError, nil
	case "warn":
		return logLevelWarn, nil
	case "debug":
		return logLevelDebug, nil
	case "trace":
		return logLevelTrace, nil
	default:
		return 0, fmt.Errorf("log_level must be one of error, warn, info, debug, or trace")
	}
}

func (l logLevel) String() string {
	switch l {
	case logLevelError:
		return "error"
	case logLevelWarn:
		return "warn"
	case logLevelDebug:
		return "debug"
	case logLevelTrace:
		return "trace"
	default:
		return "info"
	}
}

type logEvent struct {
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

type pluginLogger struct {
	mu     sync.RWMutex
	level  logLevel
	output func(logEvent)
}

func newPluginLogger(output func(logEvent)) *pluginLogger {
	return &pluginLogger{level: logLevelInfo, output: output}
}

func (l *pluginLogger) setLevel(level logLevel) {
	l.mu.Lock()
	l.level = level
	l.mu.Unlock()
}

// enabled reports whether a log at this level would be emitted, so callers can
// skip building fields or taking queue snapshots on hot paths.
func (l *pluginLogger) enabled(level logLevel) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level <= l.level && l.output != nil
}

func (l *pluginLogger) log(level logLevel, message string, fields map[string]any) {
	l.mu.RLock()
	enabled := level <= l.level
	output := l.output
	l.mu.RUnlock()
	if !enabled || output == nil {
		return
	}
	eventFields := make(map[string]any, len(fields))
	for key, value := range fields {
		eventFields[key] = value
	}
	output(logEvent{Level: level.String(), Message: message, Fields: eventFields})
}
