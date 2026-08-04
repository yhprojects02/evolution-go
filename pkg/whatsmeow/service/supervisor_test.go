package whatsmeow_service

import (
	"testing"
	"time"
)

func TestTerminalDisconnectReasonsAreNotRetried(t *testing.T) {
	terminal := []string{
		"401",
		"Logged out",
		"device_removed",
		"403",
		"banned",
		disconnectReasonRelinkRequired,
	}
	for _, reason := range terminal {
		if !isTerminalDisconnectReason(reason) {
			t.Errorf("%q should be terminal: reconnecting cannot restore revoked credentials", reason)
		}
	}
}

func TestRecoverableDisconnectReasonsAreRetried(t *testing.T) {
	recoverable := []string{
		"",
		"Reconnecting",
		"Disconnected emitted because the websocket is closed by the server.",
		"503",
		"stream error",
	}
	for _, reason := range recoverable {
		if isTerminalDisconnectReason(reason) {
			t.Errorf("%q should be retried: it is a transient drop", reason)
		}
	}
}

func TestReviveLimiterAdmitsOneAttemptAtATime(t *testing.T) {
	limiter := newReviveLimiter()
	now := time.Now()

	if !limiter.begin("instance", now) {
		t.Fatal("first attempt was refused")
	}
	// A status poll every 2.5s must not pile restarts onto the one in flight.
	if limiter.begin("instance", now.Add(3*time.Second)) {
		t.Fatal("a second attempt started while the first was still running")
	}
}

func TestReviveLimiterBacksOffAfterFailure(t *testing.T) {
	limiter := newReviveLimiter()
	now := time.Now()

	limiter.begin("instance", now)
	limiter.finish("instance", false, now)

	if limiter.begin("instance", now.Add(time.Second)) {
		t.Fatal("retried immediately after a failure instead of backing off")
	}
	if !limiter.begin("instance", now.Add(supervisorFirstBackoff+time.Second)) {
		t.Fatal("never retried after the backoff elapsed")
	}
}

func TestReviveLimiterBackoffGrowsAndIsCapped(t *testing.T) {
	limiter := newReviveLimiter()
	now := time.Now()

	for i := 0; i < 20; i++ {
		limiter.begin("instance", now)
		limiter.finish("instance", false, now)
		now = now.Add(supervisorMaxBackoff * 2)
	}

	limiter.mu.Lock()
	backoff := limiter.state["instance"].backoff
	limiter.mu.Unlock()

	if backoff != supervisorMaxBackoff {
		t.Fatalf("backoff = %s, want it capped at %s", backoff, supervisorMaxBackoff)
	}
}

func TestReviveLimiterClearsBackoffOnSuccess(t *testing.T) {
	limiter := newReviveLimiter()
	now := time.Now()

	limiter.begin("instance", now)
	limiter.finish("instance", false, now)
	limiter.begin("instance", now.Add(supervisorFirstBackoff+time.Second))
	limiter.finish("instance", true, now.Add(supervisorFirstBackoff+time.Second))

	if !limiter.begin("instance", now.Add(supervisorFirstBackoff+2*time.Second)) {
		t.Fatal("a recovered instance is still serving its old backoff")
	}
}

func TestDownClockStartsOnFirstSightingAndSurvivesSweeps(t *testing.T) {
	limiter := newReviveLimiter()
	start := time.Now()

	if down := limiter.downFor("instance", start); down != 0 {
		t.Fatalf("first sighting reported %s offline, want 0", down)
	}
	// Two sweeps later it is still inside the grace window, so whatsmeow's own
	// reconnect keeps the field to itself.
	if down := limiter.downFor("instance", start.Add(2*time.Minute)); down >= supervisorGrace {
		t.Fatalf("down = %s, want less than the %s grace", down, supervisorGrace)
	}
	if down := limiter.downFor("instance", start.Add(supervisorGrace+time.Second)); down < supervisorGrace {
		t.Fatalf("down = %s, want at least the %s grace", down, supervisorGrace)
	}
}

func TestDownClockRestartsAfterRecovery(t *testing.T) {
	limiter := newReviveLimiter()
	start := time.Now()

	limiter.downFor("instance", start)
	limiter.up("instance")

	// A later drop is a NEW outage and gets its own grace window, rather than
	// inheriting the first one and being restarted immediately.
	if down := limiter.downFor("instance", start.Add(time.Hour)); down != 0 {
		t.Fatalf("a fresh outage reported %s offline, want 0", down)
	}
}

func TestReviveLimiterResetClearsState(t *testing.T) {
	limiter := newReviveLimiter()
	now := time.Now()

	limiter.begin("instance", now)
	limiter.finish("instance", false, now)

	// A deliberate login must not have to wait out a reconnect backoff.
	limiter.Reset("instance")

	if !limiter.begin("instance", now) {
		t.Fatal("Reset did not clear the backoff")
	}
}
