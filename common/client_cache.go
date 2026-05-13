package common

import (
	"context"
	"io"
	"sync"

	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// ClientCache store initialized clients
	ClientCache interface {
		Lookup(key string, index int) (string, error) // pass through to keyResolver
		GetClientForKey(key string, index int) (any, error)
		GetClientForClientKey(clientKey string) (any, error)
		GetAllClients() ([]any, error)
	}

	keyResolver interface {
		Lookup(key string, index int) (string, error)
		GetAllAddresses() ([]string, error)
	}

	// The returned io.Closer (if non-nil) is invoked when the entry is evicted.
	clientProvider func(clientKey string) (any, io.Closer, error)

	// MembershipSubscription decouples ClientCache from membership package to
	// avoid an import cycle via common/testing/nettest. Subscribe registers a
	// notify channel and returns an unsubscribe func; CurrentAddresses returns
	// addresses currently in the ring.
	MembershipSubscription struct {
		Subscribe        func(notifyCh chan<- struct{}) (unsubscribe func(), err error)
		CurrentAddresses func() []string
	}

	cachedEntry struct {
		client any
		closer io.Closer
	}

	clientCacheImpl struct {
		keyResolver    keyResolver
		clientProvider clientProvider

		cacheLock sync.RWMutex
		clients   map[string]cachedEntry

		membership MembershipSubscription
		logger     log.Logger
		goros      goro.Group
	}
)

// NewClientCache creates a new client cache. If membership.Subscribe is
// non-nil, the cache evicts entries (closing their resource) when their
// address leaves the ring.
func NewClientCache(
	keyResolver keyResolver,
	provider clientProvider,
	membership MembershipSubscription,
	logger log.Logger,
) ClientCache {
	c := &clientCacheImpl{
		keyResolver:    keyResolver,
		clientProvider: provider,
		clients:        make(map[string]cachedEntry),
		membership:     membership,
		logger:         logger,
	}
	if membership.Subscribe != nil {
		c.goros.Go(c.eventLoop)
	}
	return c
}

func (c *clientCacheImpl) Lookup(key string, index int) (string, error) {
	return c.keyResolver.Lookup(key, index)
}

func (c *clientCacheImpl) GetClientForKey(key string, index int) (any, error) {
	clientKey, err := c.Lookup(key, index)
	if err != nil {
		return nil, err
	}
	return c.GetClientForClientKey(clientKey)
}

func (c *clientCacheImpl) GetClientForClientKey(clientKey string) (any, error) {
	c.cacheLock.RLock()
	entry, ok := c.clients[clientKey]
	c.cacheLock.RUnlock()
	if ok {
		return entry.client, nil
	}

	c.cacheLock.Lock()
	defer c.cacheLock.Unlock()

	entry, ok = c.clients[clientKey]
	if ok {
		return entry.client, nil
	}

	client, closer, err := c.clientProvider(clientKey)
	if err != nil {
		return nil, err
	}
	c.clients[clientKey] = cachedEntry{client: client, closer: closer}
	return client, nil
}

func (c *clientCacheImpl) GetAllClients() ([]any, error) {
	var result []any
	allAddresses, err := c.keyResolver.GetAllAddresses()
	if err != nil {
		return nil, err
	}
	for _, addr := range allAddresses {
		client, err := c.GetClientForClientKey(addr)
		if err != nil {
			return nil, err
		}
		result = append(result, client)
	}

	return result, nil
}

func (c *clientCacheImpl) evict(clientKey string) {
	c.cacheLock.Lock()
	entry, ok := c.clients[clientKey]
	if ok {
		delete(c.clients, clientKey)
	}
	c.cacheLock.Unlock()

	if ok && entry.closer != nil {
		if err := entry.closer.Close(); err != nil && c.logger != nil {
			c.logger.Warn("Error closing evicted client resource", tag.Error(err))
		}
	}
}

func (c *clientCacheImpl) eventLoop(ctx context.Context) error {
	notifyCh := make(chan struct{}, 1)
	unsubscribe, err := c.membership.Subscribe(notifyCh)
	if err != nil {
		if c.logger != nil {
			c.logger.Error("Error subscribing to membership", tag.Error(err))
		}
		return err
	}
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-notifyCh:
			c.evictStale()
		}
	}
}

func (c *clientCacheImpl) evictStale() {
	addresses := c.membership.CurrentAddresses()
	live := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		live[addr] = struct{}{}
	}

	c.cacheLock.Lock()
	var stale []string
	for addr := range c.clients {
		if _, ok := live[addr]; !ok {
			stale = append(stale, addr)
		}
	}
	c.cacheLock.Unlock()

	for _, addr := range stale {
		c.evict(addr)
	}
}
