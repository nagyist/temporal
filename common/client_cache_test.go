package common

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

type fakeKeyResolver struct{}

func (f *fakeKeyResolver) Lookup(_ string, _ int) (string, error) { return "", errors.New("unused") }
func (f *fakeKeyResolver) GetAllAddresses() ([]string, error)     { return nil, nil }

func makeReleaseTracker() (func() error, func() bool) {
	var released int32
	return func() error {
			atomic.AddInt32(&released, 1)
			return nil
		}, func() bool {
			return atomic.LoadInt32(&released) > 0
		}
}

func TestClientCache_EvictsStaleEntriesOnMembershipChange(t *testing.T) {
	release1, released1 := makeReleaseTracker()
	release2, released2 := makeReleaseTracker()
	releasesByKey := map[string]func() error{
		"addr1:7235": release1,
		"addr2:7235": release2,
	}

	provider := func(clientKey string) (any, func() error, error) {
		return struct{}{}, releasesByKey[clientKey], nil
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

	require.Eventually(t, released1, 2*time.Second, 10*time.Millisecond,
		"expected addr1 entry to be evicted and released")
	require.False(t, released2(), "expected addr2 to remain cached")
}

func TestClientCache_EvictReleasesResource(t *testing.T) {
	release, released := makeReleaseTracker()
	provider := func(clientKey string) (any, func() error, error) {
		return "client-for-" + clientKey, release, nil
	}

	cache := NewClientCache(&fakeKeyResolver{}, provider, MembershipSubscription{}, log.NewNoopLogger())

	c, err := cache.GetClientForClientKey("addr:1234")
	require.NoError(t, err)
	require.Equal(t, "client-for-addr:1234", c)

	cache.(*clientCacheImpl).evict("addr:1234")
	require.True(t, released())
}

func TestClientCache_NilReleaseFnIsSafe(t *testing.T) {
	provider := func(clientKey string) (any, func() error, error) {
		return clientKey, nil, nil
	}
	cache := NewClientCache(&fakeKeyResolver{}, provider, MembershipSubscription{}, log.NewNoopLogger())
	c, err := cache.GetClientForClientKey("addr")
	require.NoError(t, err)
	require.Equal(t, "addr", c)
	cache.(*clientCacheImpl).evict("addr")
}
