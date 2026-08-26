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
	ErrAborted     = errors.New("request was completed before admission")
)

// orphanTTL bounds how long an admission may stay accounted for without a
// completion. The host drops request.complete when a request record is rotated,
// so without this backstop such an admission would hold its slot forever.
const orphanTTL = 30 * time.Minute

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

// waiterResult records why a waiter was woken. It is written under q.mu before
// done is closed, so a reader that observed the close also observes the result.
type waiterResult uint8

const (
	waiterAdmitted waiterResult = iota
	waiterDisabled
	waiterAborted
)

func (r waiterResult) err() error {
	switch r {
	case waiterDisabled:
		return ErrQueueBypass
	case waiterAborted:
		return ErrAborted
	default:
		return nil
	}
}

type waiter struct {
	requestID string
	result    waiterResult
	done      chan struct{}
}

type credentialQueue struct {
	mu       sync.Mutex
	policy   providerPolicy
	disabled bool
	active   int
	queued   []*waiter
	admitted map[string]time.Time
	// tombstones holds requests whose completion arrived before acquire reached
	// the queue, so the late acquire refuses admission instead of leaking a slot.
	tombstones map[string]time.Time
	rpm        []time.Time
}

type queueSnapshot struct {
	active int
	queued int
	rpm    int
}

func newCredentialQueue(policy providerPolicy) *credentialQueue {
	return &credentialQueue{
		policy:     policy.normalized(),
		admitted:   make(map[string]time.Time),
		tombstones: make(map[string]time.Time),
	}
}

func (q *credentialQueue) acquire(ctx context.Context, requestID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	q.mu.Lock()
	q.pruneOrphansLocked()
	if _, aborted := q.tombstones[requestID]; aborted {
		delete(q.tombstones, requestID)
		q.mu.Unlock()
		return ErrAborted
	}
	if _, ok := q.admitted[requestID]; ok {
		// Host model fallback retries reuse the request ID after admission. Each
		// retry is a real upstream call, so it must widen the rpm window, but it
		// must not take another slot or block: terminating an in-flight fallback
		// would change the host retry semantics.
		q.rpm = append(q.rpm, time.Now())
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

	wake := time.NewTimer(q.nextWake(requestID))
	defer wake.Stop()

	for {
		select {
		case <-w.done:
			return w.result.err()
		case <-ctx.Done():
			if result, resolved := q.cancelWaiter(w); resolved {
				return result.err()
			}
			return ctx.Err()
		case <-deadline.C:
			if result, resolved := q.cancelWaiter(w); resolved {
				return result.err()
			}
			return ErrWaitTimeout
		case <-wake.C:
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
			// The timer already fired, so Reset needs no drain.
			wake.Reset(q.nextWake(requestID))
		}
	}
}

// nextWake reports how long this waiter may sleep before re-checking admission.
// Only the head waiter can be promoted, so the rest fall back to a slow poll.
func (q *credentialQueue) nextWake(requestID string) time.Duration {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.queued) > 0 && q.queued[0].requestID != requestID {
		return time.Second
	}
	if len(q.rpm) == 0 || q.policy.RPM <= 0 || len(q.rpm) < q.policy.RPM {
		return 250 * time.Millisecond
	}
	delay := time.Until(q.rpm[0].Add(time.Minute))
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (q *credentialQueue) promoteLocked(requestID string) bool {
	q.pruneRPM()
	q.pruneOrphansLocked()
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
		q.admitted[requestID] = time.Now()
		w.result = waiterAdmitted
		close(w.done)
		return true
	}
	q.active++
	q.rpm = append(q.rpm, time.Now())
	q.admitted[requestID] = time.Now()
	return true
}

// cancelWaiter removes target from the queue. It reports the terminal result
// when the waiter was already resolved by another goroutine.
func (q *credentialQueue) cancelWaiter(target *waiter) (waiterResult, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, admitted := q.admitted[target.requestID]; admitted {
		return waiterAdmitted, true
	}
	for i, w := range q.queued {
		if w == target {
			q.queued = append(q.queued[:i], q.queued[i+1:]...)
			return waiterAdmitted, false
		}
	}
	select {
	case <-target.done:
		return target.result, true
	default:
		return waiterAdmitted, false
	}
}

func (q *credentialQueue) snapshot() queueSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pruneRPM()
	q.pruneOrphansLocked()
	return queueSnapshot{active: q.active, queued: len(q.queued), rpm: len(q.rpm)}
}

func (q *credentialQueue) release(requestID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.releaseLocked(requestID)
}

// abort reclaims whatever requestID holds. It is idempotent across every stage
// an admission can be in: not yet queued, queued, admitted, or already gone.
func (q *credentialQueue) abort(requestID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.admitted[requestID]; ok {
		return q.releaseLocked(requestID)
	}
	for i, w := range q.queued {
		if w.requestID == requestID {
			q.queued = append(q.queued[:i], q.queued[i+1:]...)
			w.result = waiterAborted
			close(w.done)
			return false
		}
	}
	q.tombstones[requestID] = time.Now()
	return false
}

func (q *credentialQueue) releaseLocked(requestID string) bool {
	if _, ok := q.admitted[requestID]; !ok {
		return false
	}
	delete(q.admitted, requestID)
	if q.active > 0 {
		q.active--
	}
	q.pruneRPM()
	q.pruneOrphansLocked()
	if len(q.queued) > 0 {
		if q.disabled {
			return true
		}
		q.promoteLocked(q.queued[0].requestID)
	}
	return true
}

func (q *credentialQueue) pruneRPM() {
	cutoff := time.Now().Add(-time.Minute)
	first := 0
	for first < len(q.rpm) && q.rpm[first].Before(cutoff) {
		first++
	}
	if first > 0 {
		n := copy(q.rpm, q.rpm[first:])
		q.rpm = q.rpm[:n]
	}
}

func (q *credentialQueue) pruneOrphansLocked() {
	if len(q.admitted) == 0 && len(q.tombstones) == 0 {
		return
	}
	cutoff := time.Now().Add(-orphanTTL)
	for requestID, admittedAt := range q.admitted {
		if admittedAt.After(cutoff) {
			continue
		}
		delete(q.admitted, requestID)
		if q.active > 0 {
			q.active--
		}
		logger.log(logLevelWarn, "reclaimed admission without completion", map[string]any{
			"request_id": requestID,
			"reason":     "no completion within " + orphanTTL.String(),
		})
	}
	for requestID, abortedAt := range q.tombstones {
		if !abortedAt.After(cutoff) {
			delete(q.tombstones, requestID)
		}
	}
}

// requestEntry tracks the credentials one request holds or is waiting for. The
// timestamp lets abandoned entries be reclaimed when no completion arrives.
type requestEntry struct {
	authIDs map[string]struct{}
	created time.Time
}

type queueManager struct {
	mu        sync.Mutex
	policies  map[string]providerPolicy
	queues    map[string]*credentialQueue
	providers map[string]string
	requests  map[string]*requestEntry
	warned    map[string]struct{}
}

func newQueueManager() *queueManager {
	return &queueManager{
		policies:  map[string]providerPolicy{},
		queues:    map[string]*credentialQueue{},
		providers: map[string]string{},
		requests:  map[string]*requestEntry{},
		warned:    map[string]struct{}{},
	}
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
	m.warned = map[string]struct{}{}
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
		for _, w := range q.queued {
			w.result = waiterDisabled
			close(w.done)
		}
		q.queued = nil
		q.mu.Unlock()
	}
	return nil
}

// reset drops all queue state in place. Reassigning the global manager instead
// would race with in-flight calls that already captured the old pointer.
func (m *queueManager) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, q := range m.queues {
		q.mu.Lock()
		q.disabled = true
		for _, w := range q.queued {
			w.result = waiterDisabled
			close(w.done)
		}
		q.queued = nil
		q.mu.Unlock()
	}
	m.policies = map[string]providerPolicy{}
	m.queues = map[string]*credentialQueue{}
	m.providers = map[string]string{}
	m.requests = map[string]*requestEntry{}
	m.warned = map[string]struct{}{}
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

// warnOnce reports whether this reason/credential pair still deserves a warning.
// Repeat occurrences stay at trace level so a persistent gap is not a log flood.
func (m *queueManager) warnOnce(reason, authID string) bool {
	key := reason + "\x00" + authID
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.warned[key]; ok {
		return false
	}
	m.warned[key] = struct{}{}
	return true
}

func (m *queueManager) logBypass(message, reason, requestID, authID, provider string) {
	level := logLevelTrace
	if m.warnOnce(reason, authID) {
		level = logLevelWarn
	}
	if !logger.enabled(level) {
		return
	}
	fields := map[string]any{
		"request_id": requestID,
		"credential": authID,
		"reason":     reason,
		"impact":     "local queue limits are not applied to this request",
	}
	if provider != "" {
		fields["provider"] = provider
	}
	logger.log(level, message, fields)
}

func (m *queueManager) acquire(ctx context.Context, requestID, authID string) error {
	authID = strings.TrimSpace(authID)
	m.mu.Lock()
	provider, mapped := m.providers[authID]
	if !mapped || provider == "" {
		m.mu.Unlock()
		m.logBypass("request bypassed local queue: credential provider is unknown", "provider mapping unavailable", requestID, authID, "")
		return nil
	}
	policy, configured := m.policies[provider]
	if !configured {
		m.mu.Unlock()
		m.logBypass("request bypassed local queue", "provider not configured", requestID, authID, provider)
		return nil
	}
	q := m.queues[authID]
	if q == nil {
		q = newCredentialQueue(policy)
		m.queues[authID] = q
	}
	// Register before waiting: a completion that arrives while this request is
	// still queued must be able to find and reclaim it.
	m.trackLocked(requestID, authID)
	m.pruneRequestsLocked()
	m.mu.Unlock()

	if err := q.acquire(ctx, requestID); err != nil {
		m.untrack(requestID, authID)
		if errors.Is(err, ErrAborted) {
			if logger.enabled(logLevelTrace) {
				logger.log(logLevelTrace, "request abandoned before admission", map[string]any{
					"request_id": requestID,
					"credential": authID,
					"provider":   provider,
					"reason":     "completion arrived before the request was admitted",
				})
			}
			// The host already gave up on this request, so there is no upstream
			// call to gate; passing through is the safest outcome.
			return nil
		}
		if errors.Is(err, ErrQueueBypass) {
			if logger.enabled(logLevelTrace) {
				logger.log(logLevelTrace, "request bypassed local queue after reconfiguration", map[string]any{
					"request_id": requestID,
					"credential": authID,
					"provider":   provider,
					"reason":     "provider no longer configured",
				})
			}
			return nil
		}
		if logger.enabled(logLevelWarn) {
			fields := queueLogFields(requestID, authID, provider, q.snapshot())
			fields["reason"] = err.Error()
			logger.log(logLevelWarn, "request rejected by local queue", fields)
		}
		return err
	}
	if logger.enabled(logLevelDebug) {
		logger.log(logLevelDebug, "request admitted by local queue", queueLogFields(requestID, authID, provider, q.snapshot()))
	}
	return nil
}

func (m *queueManager) trackLocked(requestID, authID string) {
	entry := m.requests[requestID]
	if entry == nil {
		entry = &requestEntry{authIDs: make(map[string]struct{}), created: time.Now()}
		m.requests[requestID] = entry
	}
	entry.authIDs[authID] = struct{}{}
}

func (m *queueManager) untrack(requestID, authID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry := m.requests[requestID]
	if entry == nil {
		return
	}
	delete(entry.authIDs, authID)
	if len(entry.authIDs) == 0 {
		delete(m.requests, requestID)
	}
}

func (m *queueManager) pruneRequestsLocked() {
	cutoff := time.Now().Add(-orphanTTL)
	for requestID, entry := range m.requests {
		if !entry.created.After(cutoff) {
			delete(m.requests, requestID)
		}
	}
}

func (m *queueManager) release(requestID string) {
	m.mu.Lock()
	entry := m.requests[requestID]
	delete(m.requests, requestID)
	type queueRelease struct {
		authID   string
		provider string
		queue    *credentialQueue
	}
	var queues []queueRelease
	if entry != nil {
		queues = make([]queueRelease, 0, len(entry.authIDs))
		for authID := range entry.authIDs {
			if q := m.queues[authID]; q != nil {
				queues = append(queues, queueRelease{authID: authID, provider: m.providers[authID], queue: q})
			}
		}
	}
	m.mu.Unlock()
	for _, item := range queues {
		if !item.queue.abort(requestID) {
			continue
		}
		if logger.enabled(logLevelDebug) {
			logger.log(logLevelDebug, "request released from local queue", queueLogFields(requestID, item.authID, item.provider, item.queue.snapshot()))
		}
	}
}

func queueLogFields(requestID, authID, provider string, snapshot queueSnapshot) map[string]any {
	return map[string]any{
		"request_id": requestID,
		"credential": authID,
		"provider":   provider,
		"active":     snapshot.active,
		"queued":     snapshot.queued,
		"rpm":        snapshot.rpm,
	}
}
