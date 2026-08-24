package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrQueueFull   = errors.New("credential queue is full")
	ErrWaitTimeout = errors.New("credential queue wait timed out")
	ErrQueueBypass = errors.New("credential queue is no longer configured")
)

type providerPolicy struct {
	MaxConcurrency int           `yaml:"max_concurrency"`
	RPM            int           `yaml:"rpm"`
	MaxQueue       int           `yaml:"max_queue"`
	MaxWait        time.Duration `yaml:"-"`
	MaxWaitText    string        `yaml:"max_wait"`
}

func (p providerPolicy) normalized() providerPolicy {
	if p.MaxWait <= 0 && strings.TrimSpace(p.MaxWaitText) != "" {
		if parsed, err := time.ParseDuration(strings.TrimSpace(p.MaxWaitText)); err == nil {
			p.MaxWait = parsed
		}
	}
	return p
}

func (p providerPolicy) validate(provider string) error {
	p = p.normalized()
	if p.MaxConcurrency < 1 {
		return fmt.Errorf("provider %q max_concurrency must be greater than zero", provider)
	}
	if p.RPM < 1 {
		return fmt.Errorf("provider %q rpm must be greater than zero", provider)
	}
	if p.MaxQueue < 0 {
		return fmt.Errorf("provider %q max_queue must not be negative", provider)
	}
	if p.MaxWait <= 0 {
		return fmt.Errorf("provider %q max_wait must be greater than zero", provider)
	}
	return nil
}

type waiter struct {
	requestID string
	done      chan struct{}
}

type credentialQueue struct {
	mu       sync.Mutex
	policy   providerPolicy
	disabled bool
	active   int
	queued   []*waiter
	admitted map[string]struct{}
	rpm      []time.Time
}

func newCredentialQueue(policy providerPolicy) *credentialQueue {
	return &credentialQueue{policy: policy.normalized(), admitted: make(map[string]struct{})}
}

func (q *credentialQueue) acquire(ctx context.Context, requestID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.mu.Lock()
	if _, ok := q.admitted[requestID]; ok {
		q.mu.Unlock()
		return nil
	}
	if q.promoteLocked(requestID) {
		q.mu.Unlock()
		return nil
	}
	if len(q.queued) >= q.policy.MaxQueue {
		q.mu.Unlock()
		return ErrQueueFull
	}
	w := &waiter{requestID: requestID, done: make(chan struct{})}
	q.queued = append(q.queued, w)
	deadline := time.NewTimer(q.policy.MaxWait)
	q.mu.Unlock()
	defer deadline.Stop()

	for {
		select {
		case <-w.done:
			q.mu.Lock()
			disabled := q.disabled
			q.mu.Unlock()
			if disabled {
				return ErrQueueBypass
			}
			return nil
		case <-ctx.Done():
			if q.cancelWaiter(w) {
				return nil
			}
			if q.isDisabled() {
				return ErrQueueBypass
			}
			return ctx.Err()
		case <-deadline.C:
			if q.cancelWaiter(w) {
				return nil
			}
			if q.isDisabled() {
				return ErrQueueBypass
			}
			return ErrWaitTimeout
		case <-q.nextWake():
			q.mu.Lock()
			if q.disabled {
				q.mu.Unlock()
				return ErrQueueBypass
			}
			if q.promoteLocked(requestID) {
				q.mu.Unlock()
				return nil
			}
			q.mu.Unlock()
		}
	}
}

func (q *credentialQueue) nextWake() <-chan time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.rpm) == 0 || q.policy.RPM <= 0 || len(q.rpm) < q.policy.RPM {
		return time.After(250 * time.Millisecond)
	}
	delay := time.Until(q.rpm[0].Add(time.Minute))
	if delay < 0 {
		delay = 0
	}
	return time.After(delay)
}

func (q *credentialQueue) promoteLocked(requestID string) bool {
	q.pruneRPM()
	if q.active >= q.policy.MaxConcurrency || len(q.rpm) >= q.policy.RPM {
		return false
	}
	if len(q.queued) > 0 {
		if q.queued[0].requestID != requestID {
			return false
		}
		w := q.queued[0]
		q.queued = q.queued[1:]
		q.active++
		q.rpm = append(q.rpm, time.Now())
		q.admitted[requestID] = struct{}{}
		close(w.done)
		return true
	}
	q.active++
	q.rpm = append(q.rpm, time.Now())
	q.admitted[requestID] = struct{}{}
	return true
}

func (q *credentialQueue) cancelWaiter(target *waiter) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, admitted := q.admitted[target.requestID]; admitted {
		return true
	}
	for i, w := range q.queued {
		if w == target {
			q.queued = append(q.queued[:i], q.queued[i+1:]...)
			return false
		}
	}
	return false
}

func (q *credentialQueue) isDisabled() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.disabled
}

func (q *credentialQueue) release(requestID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.admitted[requestID]; !ok {
		return
	}
	delete(q.admitted, requestID)
	if q.active > 0 {
		q.active--
	}
	q.pruneRPM()
	if len(q.queued) > 0 {
		if q.disabled {
			return
		}
		q.promoteLocked(q.queued[0].requestID)
	}
}

func (q *credentialQueue) pruneRPM() {
	cutoff := time.Now().Add(-time.Minute)
	first := 0
	for first < len(q.rpm) && q.rpm[first].Before(cutoff) {
		first++
	}
	if first > 0 {
		q.rpm = append([]time.Time(nil), q.rpm[first:]...)
	}
}

type queueManager struct {
	mu        sync.Mutex
	policies  map[string]providerPolicy
	queues    map[string]*credentialQueue
	providers map[string]string
	requests  map[string]map[string]struct{}
}

func newQueueManager() *queueManager {
	return &queueManager{policies: map[string]providerPolicy{}, queues: map[string]*credentialQueue{}, providers: map[string]string{}, requests: map[string]map[string]struct{}{}}
}

func (m *queueManager) configure(policies map[string]providerPolicy) error {
	next := make(map[string]providerPolicy, len(policies))
	for provider, policy := range policies {
		provider = strings.ToLower(strings.TrimSpace(provider))
		if provider == "" {
			return errors.New("provider name must not be empty")
		}
		policy = policy.normalized()
		if err := policy.validate(provider); err != nil {
			return err
		}
		next[provider] = policy
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.policies = next
	for authID, q := range m.queues {
		provider := m.providers[authID]
		if policy, ok := next[provider]; ok {
			q.mu.Lock()
			q.disabled = false
			q.policy = policy
			q.mu.Unlock()
			continue
		}
		q.mu.Lock()
		q.disabled = true
		for _, waiter := range q.queued {
			close(waiter.done)
		}
		q.queued = nil
		q.mu.Unlock()
	}
	return nil
}

func (m *queueManager) rememberCandidates(providerByAuth map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for authID, provider := range providerByAuth {
		m.providers[strings.TrimSpace(authID)] = strings.ToLower(strings.TrimSpace(provider))
	}
}

func (m *queueManager) policyFor(provider string) (providerPolicy, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	policy, ok := m.policies[strings.ToLower(strings.TrimSpace(provider))]
	return policy, ok
}

func (m *queueManager) acquire(ctx context.Context, requestID, authID string) error {
	m.mu.Lock()
	provider := m.providers[strings.TrimSpace(authID)]
	policy, configured := m.policies[provider]
	if !configured {
		m.mu.Unlock()
		return nil
	}
	q := m.queues[authID]
	if q == nil {
		q = newCredentialQueue(policy)
		m.queues[authID] = q
	}
	m.mu.Unlock()
	if err := q.acquire(ctx, requestID); err != nil {
		if errors.Is(err, ErrQueueBypass) {
			return nil
		}
		return err
	}
	m.mu.Lock()
	if m.requests[requestID] == nil {
		m.requests[requestID] = make(map[string]struct{})
	}
	m.requests[requestID][authID] = struct{}{}
	m.mu.Unlock()
	return nil
}

func (m *queueManager) release(requestID string) {
	m.mu.Lock()
	authIDs := m.requests[requestID]
	delete(m.requests, requestID)
	queues := make([]*credentialQueue, 0, len(authIDs))
	for authID := range authIDs {
		if q := m.queues[authID]; q != nil {
			queues = append(queues, q)
		}
	}
	m.mu.Unlock()
	for _, q := range queues {
		q.release(requestID)
	}
}
