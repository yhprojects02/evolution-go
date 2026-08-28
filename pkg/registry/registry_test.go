package registry

import (
	"sync"
	"testing"
	"time"
)

// TestClientsConcurrentAccess is the regression the whole package exists for:
// with a bare map this pattern aborts the process with
// "fatal error: concurrent map writes". Run it under -race.
func TestClientsConcurrentAccess(t *testing.T) {
	clients := NewClients()

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			id := string(rune('a' + worker))
			for i := 0; i < 500; i++ {
				clients.Set(id, nil)
				clients.Get(id)
				clients.Lookup(id)
				clients.Snapshot()
				clients.Len()
				clients.Delete(id)
			}
		}(worker)
	}
	wg.Wait()
}

func TestKillChannelSignalIsBroadcastAndIdempotent(t *testing.T) {
	kills := NewKillChannels()
	stop := kills.Ensure("instance")

	// Two independent waiters, exactly like the supervisor loop and the
	// presence keepalive goroutine.
	received := make(chan string, 2)
	for _, name := range []string{"supervisor", "presence"} {
		go func(name string) {
			<-stop
			received <- name
		}(name)
	}

	kills.Signal("instance")
	// A second stop from a racing teardown path must not panic.
	kills.Signal("instance")

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case name := <-received:
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 waiters were woken: %v", len(seen), seen)
		}
	}
}

func TestKillChannelSignalNeverBlocksWithoutAReceiver(t *testing.T) {
	kills := NewKillChannels()
	kills.Ensure("instance")

	done := make(chan struct{})
	go func() {
		// The QR-timeout path signalled with nobody receiving yet and
		// deadlocked the goroutine forever.
		kills.Signal("instance")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Signal blocked when no goroutine was receiving")
	}
}

func TestKillChannelResetStartsANewGeneration(t *testing.T) {
	kills := NewKillChannels()
	old := kills.Ensure("instance")
	kills.Signal("instance")

	fresh := kills.Reset("instance")
	if fresh == old {
		t.Fatal("Reset returned the stopped channel")
	}
	select {
	case <-fresh:
		t.Fatal("a freshly reset channel is already stopped")
	default:
	}
}

func TestKillChannelLookupDoesNotResurrectDeletedGeneration(t *testing.T) {
	kills := NewKillChannels()
	kills.Reset("instance")
	kills.Signal("instance")
	kills.Delete("instance")

	if _, ok := kills.Lookup("instance"); ok {
		t.Fatal("Lookup found a kill channel after deletion")
	}
}

func TestSignalReportsWhetherAChannelExisted(t *testing.T) {
	kills := NewKillChannels()
	if kills.Signal("missing") {
		t.Fatal("signalled an instance that was never registered")
	}
	kills.Ensure("present")
	if !kills.Signal("present") {
		t.Fatal("failed to signal a registered instance")
	}
}
