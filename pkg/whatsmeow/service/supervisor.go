package whatsmeow_service

import (
	"context"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// disconnectReasonRelinkRequired marks an instance WhatsApp has unlinked. The
// supervisor will not keep retrying it, because no amount of reconnecting
// brings back credentials the server has revoked — the number has to be linked
// again from the console.
const disconnectReasonRelinkRequired = "relink required: WhatsApp removed this companion device"

// SupervisorInterval is how often every registered instance is checked against
// its live client.
const SupervisorInterval = 60 * time.Second

// supervisorFirstBackoff / supervisorMaxBackoff bound the per-instance retry
// delay. A number that keeps failing is retried ever more slowly instead of
// being hammered once a minute forever.
const (
	supervisorFirstBackoff = 30 * time.Second
	supervisorMaxBackoff   = 15 * time.Minute
)

// terminalDisconnectReasons never recover by reconnecting. Retrying them just
// generates load and, worse, can churn companion device registrations.
var terminalDisconnectReasons = []string{
	"relink required",
	"logged out",
	"loggedout",
	"device_removed",
	"device removed",
	"banned",
	"401",
	"403",
}

func isTerminalDisconnectReason(reason string) bool {
	normalized := strings.ToLower(strings.TrimSpace(reason))
	if normalized == "" {
		return false
	}
	for _, terminal := range terminalDisconnectReasons {
		if strings.Contains(normalized, terminal) {
			return true
		}
	}
	return false
}

// supervisorGrace is how long an instance must stay down before the supervisor
// restarts it.
//
// whatsmeow's own auto-reconnect is already retrying a dropped socket, and
// restarting underneath it produces two clients racing for one device. Its
// delay grows without bound though (AutoReconnectErrors × 2s), so an instance
// that has been failing for a while genuinely benefits from a fresh client
// with a reset counter. This is the line between the two.
const supervisorGrace = 3 * time.Minute

// reviveLimiter serialises restart attempts per instance and applies backoff,
// so that a status probe every 2.5s cannot turn into a restart storm.
type reviveLimiter struct {
	mu    sync.Mutex
	state map[string]*reviveState
}

type reviveState struct {
	inFlight bool
	nextTry  time.Time
	backoff  time.Duration
	downFrom time.Time
}

func newReviveLimiter() *reviveLimiter {
	return &reviveLimiter{state: make(map[string]*reviveState)}
}

// downFor records that an instance is currently offline and reports how long
// it has been offline for. The first call starts the clock.
func (r *reviveLimiter) downFor(instanceId string, now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.state[instanceId]
	if !ok {
		entry = &reviveState{}
		r.state[instanceId] = entry
	}
	if entry.downFrom.IsZero() {
		entry.downFrom = now
	}
	return now.Sub(entry.downFrom)
}

// up clears the offline clock for an instance that is connected again.
func (r *reviveLimiter) up(instanceId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.state[instanceId]; ok {
		entry.downFrom = time.Time{}
	}
}

// begin reports whether the caller may start instanceId right now. When it
// returns true the caller MUST call finish exactly once.
func (r *reviveLimiter) begin(instanceId string, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.state[instanceId]
	if !ok {
		entry = &reviveState{}
		r.state[instanceId] = entry
	}
	if entry.inFlight || now.Before(entry.nextTry) {
		return false
	}
	entry.inFlight = true
	return true
}

func (r *reviveLimiter) finish(instanceId string, succeeded bool, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.state[instanceId]
	if !ok {
		return
	}
	entry.inFlight = false
	if succeeded {
		entry.backoff = 0
		entry.nextTry = time.Time{}
		return
	}
	if entry.backoff == 0 {
		entry.backoff = supervisorFirstBackoff
	} else {
		entry.backoff *= 2
		if entry.backoff > supervisorMaxBackoff {
			entry.backoff = supervisorMaxBackoff
		}
	}
	entry.nextTry = now.Add(entry.backoff)
}

// Reset clears the backoff for an instance, e.g. after a deliberate login.
func (r *reviveLimiter) Reset(instanceId string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.state, instanceId)
}

func (w whatsmeowService) recoverGoroutine(instanceId string, where string) {
	if r := recover(); r != nil {
		w.loggerWrapper.GetLogger(instanceId).LogError(
			"[%s] Recovered from panic in %s: %v\n%s", instanceId, where, r, debug.Stack())
	}
}

// ReviveInstance brings an instance's client back if it is missing or dead,
// without blocking the caller and without stampeding. It is the single entry
// point every "this should be connected but isn't" path uses.
func (w whatsmeowService) ReviveInstance(instanceId string) {
	if !w.reviveLimiter.begin(instanceId, time.Now()) {
		return
	}

	go func() {
		defer w.recoverGoroutine(instanceId, "ReviveInstance")

		err := w.StartInstance(instanceId)
		if err != nil {
			w.loggerWrapper.GetLogger(instanceId).LogError("[%s] Revive failed: %v", instanceId, err)
			w.reviveLimiter.finish(instanceId, false, time.Now())
			return
		}

		// StartInstance only spawns the client goroutine. Give the handshake a
		// moment so a failure counts as a failure and earns its backoff,
		// instead of resetting it every minute.
		time.Sleep(8 * time.Second)
		client := w.clientPointer.Get(instanceId)
		w.reviveLimiter.finish(instanceId, client != nil && client.IsConnected(), time.Now())
	}()
}

// SuperviseInstances is the backstop that makes a dropped number heal itself.
//
// whatsmeow's own auto-reconnect covers an ordinary stream drop. It does not
// cover the cases that actually stranded numbers here: a ConnectFailure (a 503
// from WhatsApp, a version rejection), an engine restart that raced the
// database, or a client goroutine that exited without cleaning the registry.
// Once a minute this reconciles the instance table against the live registry
// and restarts anything that should be connected but is not.
func (w whatsmeowService) SuperviseInstances(ctx context.Context, clientName string, interval time.Duration) {
	defer w.recoverGoroutine(clientName, "SuperviseInstances")

	if interval <= 0 {
		interval = SupervisorInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	w.loggerWrapper.GetLogger(clientName).LogInfo("Instance supervisor running every %s", interval)

	for {
		select {
		case <-ctx.Done():
			w.loggerWrapper.GetLogger(clientName).LogInfo("Instance supervisor stopped")
			return
		case <-ticker.C:
			w.reconcileInstances(clientName)
		}
	}
}

func (w whatsmeowService) reconcileInstances(clientName string) {
	defer w.recoverGoroutine(clientName, "reconcileInstances")

	instances, err := w.instanceRepository.GetAll(clientName)
	if err != nil {
		w.loggerWrapper.GetLogger(clientName).LogError("Supervisor could not list instances: %v", err)
		return
	}

	now := time.Now()
	for _, instance := range instances {
		if instance == nil || strings.TrimSpace(instance.Jid) == "" {
			continue
		}

		if client := w.clientPointer.Get(instance.Id); client != nil && client.IsConnected() {
			w.reviveLimiter.up(instance.Id)
			continue
		}

		down := w.reviveLimiter.downFor(instance.Id, now)

		// Reconnecting cannot restore credentials WhatsApp has revoked, and
		// retrying churns the phone's companion-device slots. Leave it visibly
		// broken so it gets linked again on purpose.
		if isTerminalDisconnectReason(instance.DisconnectReason) {
			continue
		}

		// Give whatsmeow's own reconnect loop room to win first.
		if down < supervisorGrace {
			continue
		}

		w.loggerWrapper.GetLogger(instance.Id).LogWarn(
			"[%s] Supervisor found %s offline for %s (reason: %q); restarting",
			instance.Id, instance.Jid, down.Round(time.Second), instance.DisconnectReason)
		w.ReviveInstance(instance.Id)
	}
}
