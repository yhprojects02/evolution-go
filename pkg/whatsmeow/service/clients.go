package whatsmeow_service

import "sync"

// myClients is the concurrency-safe index of per-instance event-handler
// wrappers. Same reasoning as registry.Clients: this map was written by client
// goroutines while HTTP handlers read it, which the Go runtime punishes by
// aborting the process. It lives here rather than in pkg/registry only because
// MyClient is defined in this package.
type myClients struct {
	mu sync.RWMutex
	m  map[string]*MyClient
}

func newMyClients() *myClients {
	return &myClients{m: make(map[string]*MyClient)}
}

func (c *myClients) Get(id string) *MyClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.m[id]
}

func (c *myClients) Lookup(id string) (*MyClient, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	client, ok := c.m[id]
	return client, ok
}

func (c *myClients) Set(id string, client *MyClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id] = client
}

func (c *myClients) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, id)
}

// DeleteIf removes id only when the stored wrapper is still the caller's own,
// so a late teardown cannot evict a newer login for the same instance.
func (c *myClients) DeleteIf(id string, expected *MyClient) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[id] != expected {
		return false
	}
	delete(c.m, id)
	return true
}
