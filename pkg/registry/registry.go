// Package registry holds the process-wide, concurrency-safe indexes of live
// WhatsApp clients and their kill channels.
//
// These used to be bare `map[string]*whatsmeow.Client` and
// `map[string](chan bool)` values, shared by reference between every Gin
// handler goroutine, every per-instance client goroutine, and every whatsmeow
// event handler. Nothing guarded them. A bare Go map that is written from one
// goroutine while another reads or writes it is not a data race you can
// recover from: the runtime aborts the process with
// "fatal error: concurrent map writes". One unlucky interleaving between an
// HTTP request and a session starting or dropping took down every linked
// number at once, and `restart: always` then re-linked them all — which looks,
// from the outside, exactly like WhatsApp randomly disconnecting.
package registry

import (
	"sync"

	"go.mau.fi/whatsmeow"
)

// Clients maps an instance id to its live whatsmeow client.
type Clients struct {
	mu sync.RWMutex
	m  map[string]*whatsmeow.Client
}

func NewClients() *Clients {
	return &Clients{m: make(map[string]*whatsmeow.Client)}
}

// Get returns the client for id, or nil when there is none. Callers that
// previously wrote `clientPointer[id]` and relied on the zero value get the
// same behaviour, minus the race.
func (c *Clients) Get(id string) *whatsmeow.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[id]
}

// Lookup is the comma-ok form.
func (c *Clients) Lookup(id string) (*whatsmeow.Client, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	client, ok := c.m[id]
	return client, ok
}

func (c *Clients) Set(id string, client *whatsmeow.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id] = client
}

func (c *Clients) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
}

// DeleteIf removes id only when the stored client is still the one the caller
// is holding. A late cleanup path (a QR timeout finishing after the user has
// already started a fresh login) must never evict the newer client.
func (c *Clients) DeleteIf(id string, expected *whatsmeow.Client) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[id] != expected {
		return false
	}
	delete(c.m, id)
	return true
}

func (c *Clients) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.m)
}

// Snapshot copies the index so a caller can range over it without holding the
// lock across arbitrary whatsmeow calls.
func (c *Clients) Snapshot() map[string]*whatsmeow.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]*whatsmeow.Client, len(c.m))
	for id, client := range c.m {
		out[id] = client
	}
	return out
}

// killChannel is one instance's stop signal. Stopping is a broadcast — the
// supervisor loop and the presence-keepalive goroutine both wait on it — so it
// is delivered by CLOSING the channel rather than sending a value. A send only
// ever reaches one of the two waiters, which is why a stopped instance used to
// leave a keepalive goroutine running against a dead socket.
//
// The sync.Once makes Signal idempotent: several teardown paths (logout, kill,
// delete, QR timeout) can race to stop the same instance without the double
// close that would panic the process.
type killChannel struct {
	ch   chan struct{}
	once sync.Once
}

func (k *killChannel) signal() {
	k.once.Do(func() { close(k.ch) })
}

// KillChannels maps an instance id to that instance's stop signal.
type KillChannels struct {
	mu sync.RWMutex
	m  map[string]*killChannel
}

func NewKillChannels() *KillChannels {
	return &KillChannels{m: make(map[string]*killChannel)}
}

// Reset installs a fresh, unstopped signal for id and returns its channel.
// Call it when an instance starts: the previous generation's channel stays
// closed, so any goroutine still draining it exits instead of latching onto
// the new client.
func (k *KillChannels) Reset(id string) <-chan struct{} {
	entry := &killChannel{ch: make(chan struct{})}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[id] = entry
	return entry.ch
}

// Ensure returns the stop channel for id, creating one if the instance does
// not have it yet. Receivers must use this rather than reading the map
// directly: receiving from a nil channel blocks forever, which would strand a
// supervisor goroutine. Capture the result ONCE, outside the select loop — a
// per-iteration Ensure would silently resurrect a deleted channel and miss the
// stop.
func (k *KillChannels) Ensure(id string) <-chan struct{} {
	k.mu.RLock()
	entry, ok := k.m[id]
	k.mu.RUnlock()
	if ok && entry != nil {
		return entry.ch
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if entry, ok := k.m[id]; ok && entry != nil {
		return entry.ch
	}
	entry = &killChannel{ch: make(chan struct{})}
	k.m[id] = entry
	return entry.ch
}

func (k *KillChannels) Delete(id string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, id)
}

// Signal tells every goroutine watching this instance to stop. It never
// blocks and never panics on a repeat call. It reports whether a channel
// existed to signal.
func (k *KillChannels) Signal(id string) bool {
	k.mu.RLock()
	entry, ok := k.m[id]
	k.mu.RUnlock()
	if !ok || entry == nil {
		return false
	}
	entry.signal()
	return true
}
