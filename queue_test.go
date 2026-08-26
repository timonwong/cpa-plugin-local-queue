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

func TestUnknownCredentialMappingBypasses(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{"codex": {MaxConcurrency: 1, RPM: 1, MaxQueue: 0, MaxWait: time.Second}}); err != nil {
		t.Fatal(err)
	}
	if err := m.acquire(context.Background(), "r1", "unknown-auth"); err != nil {
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

func waitForQueueDepth(t *testing.T, q *credentialQueue, depth int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		q.mu.Lock()
		got := len(q.queued)
		q.mu.Unlock()
		if got == depth {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue depth: got %d, want %d", got, depth)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCompletionWhileQueuedReleasesRequestWithoutLeakingSlot(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{"codex": {MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: 5 * time.Second}}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"auth-a": "codex"})
	if err := m.acquire(context.Background(), "holder", "auth-a"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	q := m.queues["auth-a"]
	m.mu.Unlock()

	late := make(chan error, 1)
	go func() { late <- m.acquire(context.Background(), "late", "auth-a") }()
	waitForQueueDepth(t, q, 1)

	m.release("late")
	if err := <-late; err != nil {
		t.Fatalf("abandoned request returned %v", err)
	}
	m.release("holder")

	q.mu.Lock()
	active, admitted, queued := q.active, len(q.admitted), len(q.queued)
	q.mu.Unlock()
	if active != 0 || admitted != 0 || queued != 0 {
		t.Fatalf("queue state after abandonment: active=%d admitted=%d queued=%d", active, admitted, queued)
	}
	m.mu.Lock()
	tracked := len(m.requests)
	m.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("tracked requests: got %d, want 0", tracked)
	}
}

func TestCompletionBeforeAcquireRefusesAdmission(t *testing.T) {
	q := newCredentialQueue(providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: time.Second})
	q.abort("r1")
	if err := q.acquire(context.Background(), "r1"); !errors.Is(err, ErrAborted) {
		t.Fatalf("got %v, want ErrAborted", err)
	}
	q.mu.Lock()
	active, admitted, tombstones := q.active, len(q.admitted), len(q.tombstones)
	q.mu.Unlock()
	if active != 0 || admitted != 0 || tombstones != 0 {
		t.Fatalf("queue state: active=%d admitted=%d tombstones=%d", active, admitted, tombstones)
	}
}

func TestAdmissionWithoutCompletionIsReclaimedAfterTTL(t *testing.T) {
	q := newCredentialQueue(providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: time.Second})
	if err := q.acquire(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	q.admitted["r1"] = time.Now().Add(-2 * orphanTTL)
	q.mu.Unlock()

	if got := q.snapshot(); got.active != 0 {
		t.Fatalf("active after ttl prune: got %d, want 0", got.active)
	}
	q.mu.Lock()
	admitted := len(q.admitted)
	q.mu.Unlock()
	if admitted != 0 {
		t.Fatalf("admitted after ttl prune: got %d, want 0", admitted)
	}
	if err := q.acquire(context.Background(), "r2"); err != nil {
		t.Fatalf("reclaimed slot not reusable: %v", err)
	}
}

func TestWaiterWokenByDisableIsNeverSilentlyAdmitted(t *testing.T) {
	q := newCredentialQueue(providerPolicy{MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: 2 * time.Second})
	if err := q.acquire(context.Background(), "r1"); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- q.acquire(context.Background(), "r2") }()
	waitForQueueDepth(t, q, 1)

	// Disable and re-enable in one critical section: the waiter can only observe
	// the queue after it was re-enabled, so the wake reason must be explicit.
	q.mu.Lock()
	q.disabled = true
	for _, w := range q.queued {
		w.result = waiterDisabled
		close(w.done)
	}
	q.queued = nil
	q.disabled = false
	q.mu.Unlock()

	err := <-result
	q.mu.Lock()
	_, admitted := q.admitted["r2"]
	q.mu.Unlock()
	switch {
	case err == nil && !admitted:
		t.Fatal("waiter was admitted without being counted")
	case err != nil && !errors.Is(err, ErrQueueBypass):
		t.Fatalf("got %v, want ErrQueueBypass", err)
	case err != nil && admitted:
		t.Fatal("bypassed waiter still holds a slot")
	}
}

func TestResetClearsStateAndWakesWaiters(t *testing.T) {
	m := newQueueManager()
	if err := m.configure(map[string]providerPolicy{"codex": {MaxConcurrency: 1, RPM: 100, MaxQueue: 1, MaxWait: 5 * time.Second}}); err != nil {
		t.Fatal(err)
	}
	m.rememberCandidates(map[string]string{"auth-a": "codex"})
	if err := m.acquire(context.Background(), "r1", "auth-a"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	q := m.queues["auth-a"]
	m.mu.Unlock()
	result := make(chan error, 1)
	go func() { result <- m.acquire(context.Background(), "r2", "auth-a") }()
	waitForQueueDepth(t, q, 1)

	m.reset()
	if err := <-result; err != nil {
		t.Fatalf("waiter after reset returned %v", err)
	}
	m.mu.Lock()
	queues, requests, providers, policies := len(m.queues), len(m.requests), len(m.providers), len(m.policies)
	m.mu.Unlock()
	if queues != 0 || requests != 0 || providers != 0 || policies != 0 {
		t.Fatalf("reset state: queues=%d requests=%d providers=%d policies=%d", queues, requests, providers, policies)
	}
}
