package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConfiguredProviderUsesIndependentCredentialQueues(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{"codex": {MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: time.Second}}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"a": "codex", "b": "codex"})
	if err := m.acquire(context.Background(), "r1", "a"); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(context.Background(), "r2", "b"); err != nil {
		t.Fatal(err)
	}
	m.release("r1")
	m.release("r2")
}

func TestUnconfiguredProviderBypasses(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{"codex": {MaxConcurrency: 1, RPM: 1, MaxQueue: 0, MaxWait: time.Second}}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"a": "claude"})
	if err := m.acquire(context.Background(), "r1", "a"); err != nil {
		t.Fatal(err)
	}
}

func TestCredentialQueueIsFIFOAndBounded(t *testing.T) {
	q := newCredentialQueue(providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: 100 * time.Millisecond})
	if err := q.acquire(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() { queued <- q.acquire(context.Background(), "r2") }()
	deadline := time.Now().Add(time.Second)
	for {
		q.mu.Lock()
		depth := len(q.queued)
		q.mu.Unlock()
		if depth == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("r2 was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	if err := q.acquire(context.Background(), "r3"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	q.release("r1")
	if err := <-queued; err != nil {
		t.Fatal(err)
	}
	q.release("r2")
}

func TestRemovedProviderDisablesExistingQueueWaiters(t *testing.T) {
	m := newQueueManager()
	policy := providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: time.Second}
	if err := m.configure(map[string]providerPolicy{"codex": policy}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"auth-a": "codex"})
	if err := m.acquire(context.Background(), "request-a", "auth-a"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- m.acquire(context.Background(), "request-b", "auth-a") }()
	deadline := time.Now().Add(time.Second)
	for {
		m.mu.Lock()
		q := m.queues["auth-a"]
		m.mu.Unlock()
		q.mu.Lock()
		depth := len(q.queued)
		q.mu.Unlock()
		if depth == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request-b was not queued")
		}
		time.Sleep(time.Millisecond)
	}
	if err := m.configure(nil); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("removed provider waiter returned %v", err)
	}
}

func TestCredentialQueueWaitTimeout(t *testing.T) {
	q := newCredentialQueue(providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: 10 * time.Millisecond})
	if err := q.acquire(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	if err := q.acquire(context.Background(), "r2"); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("got %v, want ErrWaitTimeout", err)
	}
}
