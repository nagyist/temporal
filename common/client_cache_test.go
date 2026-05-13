package common

import (
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

type fakeKeyResolver struct{}

func (f *fakeKeyResolver) Lookup(_ string, _ int) (string, error)  { return "", errors.New("unused") }
func (f *fakeKeyResolver) GetAllAddresses() ([]string, error)      { return nil, nil }

type countingCloser struct {
	closed int32
}

func (c *countingCloser) Close() error {
	atomic.AddInt32(&c.closed, 1)
	return nil
}

func (c *countingCloser) Closed() bool {
	return atomic.LoadInt32(&c.closed) > 0
}

func TestClientCache_EvictsStaleEntriesOnMembershipChange(t *testing.T) {
	closer1 := &countingCloser{}
	closer2 := &countingCloser{}
	closersByKey := map[string]*countingCloser{
		"addr1:7235": closer1,
		"addr2:7235": closer2,
	}

	provider := func(clientKey string) (any, io.Closer, error) {
		return struct{}{}, closersByKey[clientKey], nil
	}

	subscribed := make(chan chan<- struct{}, 1)
	var liveAddrs atomic.Value
	liveAddrs.Store([]string{"addr1:7235", "addr2:7235"})

	sub := MembershipSubscription{
		Subscribe: func(ch chan<- struct{}) (func(), error) {
			subscribed <- ch
			return func() {}, nil
		},
		CurrentAddresses: func() []string {
			return liveAddrs.Load().([]string)
		},
	}

	cache := NewClientCache(&fakeKeyResolver{}, provider, sub, log.NewNoopLogger())

	_, err := cache.GetClientForClientKey("addr1:7235")
	require.NoError(t, err)
	_, err = cache.GetClientForClientKey("addr2:7235")
	require.NoError(t, err)

	// addr1 leaves the ring; addr2 remains.
	liveAddrs.Store([]string{"addr2:7235"})

	notifyCh := <-subscribed
	notifyCh <- struct{}{}

	require.Eventually(t, func() bool {
		return closer1.Closed()
	}, 2*time.Second, 10*time.Millisecond, "expected addr1 entry to be evicted and closed")

	require.False(t, closer2.Closed(), "expected addr2 to remain cached")
}

func TestClientCache_EvictRemovesEntryAndClosesResource(t *testing.T) {
	closer := &countingCloser{}
	provider := func(clientKey string) (any, io.Closer, error) {
		return "client-for-" + clientKey, closer, nil
	}

	cache := NewClientCache(&fakeKeyResolver{}, provider, MembershipSubscription{}, log.NewNoopLogger())

	c, err := cache.GetClientForClientKey("addr:1234")
	require.NoError(t, err)
	require.Equal(t, "client-for-addr:1234", c)

	cache.(*clientCacheImpl).evict("addr:1234")
	require.True(t, closer.Closed(), "evicted entry's closer should run")

	// After eviction, calling again creates a fresh entry via the provider.
	newCloser := &countingCloser{}
	provider2 := func(clientKey string) (any, io.Closer, error) {
		return "fresh-" + clientKey, newCloser, nil
	}
	cache2 := NewClientCache(&fakeKeyResolver{}, provider2, MembershipSubscription{}, log.NewNoopLogger())
	_, err = cache2.GetClientForClientKey("addr:1234")
	require.NoError(t, err)
	cache2.(*clientCacheImpl).evict("addr:1234")
	require.True(t, newCloser.Closed())
}

func TestClientCache_NilResolverSkipsListener(t *testing.T) {
	provider := func(clientKey string) (any, io.Closer, error) {
		return clientKey, nil, nil
	}
	cache := NewClientCache(&fakeKeyResolver{}, provider, MembershipSubscription{}, log.NewNoopLogger())
	c, err := cache.GetClientForClientKey("addr")
	require.NoError(t, err)
	require.Equal(t, "addr", c)
	cache.(*clientCacheImpl).evict("addr") // nil closer is safe
}
