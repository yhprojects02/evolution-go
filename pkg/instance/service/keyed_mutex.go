package instance_service

import "sync"

// keyedMutex serialises work per instance id.
//
// Login restarts must not overlap. Two clients polling the same session — two
// browser tabs, or a retry landing on top of a slow request — would otherwise
// both find no live client and both start a fresh login socket, and the second
// one tears down the first just as it publishes its QR code. The operator sees
// a code that is already dead.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newKeyedMutex() *keyedMutex {
	return &keyedMutex{locks: make(map[string]*sync.Mutex)}
}

func (k *keyedMutex) Lock(key string) func() {
	k.mu.Lock()
	lock, ok := k.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		k.locks[key] = lock
	}
	k.mu.Unlock()

	lock.Lock()
	return lock.Unlock
}
