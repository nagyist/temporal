package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeRPCFactory struct{}

func (f *fakeRPCFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	// grpc.NewClient is lazy and does not dial until an RPC is issued, so this
	// is safe to call with unreachable addresses in tests.
	conn, err := grpc.NewClient(rpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	return conn
}

func TestConnectionPool_EvictsStaleConnsOnMembershipChange(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)

	listenerCh := make(chan chan<- *membership.ChangedEvent, 1)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ string, ch chan<- *membership.ChangedEvent) error {
			listenerCh <- ch
			return nil
		},
	).Times(1)
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()

	pool := NewConnectionPool(
		resolver,
		&fakeRPCFactory{},
		historyservice.NewHistoryServiceClient,
		log.NewNoopLogger(),
	)
	defer pool.stop()

	cc1 := pool.getOrCreateClientConn("addr1:7235")
	cc2 := pool.getOrCreateClientConn("addr2:7235")

	// addr1 leaves the ring; addr2 remains.
	resolver.EXPECT().Members().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("addr2:7235"),
	}).AnyTimes()

	// Trigger the membership listener.
	notifyCh := <-listenerCh
	notifyCh <- &membership.ChangedEvent{}

	require.Eventually(t, func() bool {
		pool.mu.RLock()
		defer pool.mu.RUnlock()
		_, stillCached := pool.mu.conns["addr1:7235"]
		return !stillCached
	}, 2*time.Second, 10*time.Millisecond, "expected addr1 to be evicted from pool")

	require.Equal(t, connectivity.Shutdown, cc1.grpcConn.GetState(),
		"expected evicted gRPC conn to be closed")

	pool.mu.RLock()
	_, addr2Cached := pool.mu.conns["addr2:7235"]
	pool.mu.RUnlock()
	require.True(t, addr2Cached, "expected addr2 to remain cached")
	require.NotEqual(t, connectivity.Shutdown, cc2.grpcConn.GetState(),
		"expected addr2 conn to remain open")
}

func TestConnectionPool_EvictionIsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)

	listenerCh := make(chan chan<- *membership.ChangedEvent, 1)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ string, ch chan<- *membership.ChangedEvent) error {
			listenerCh <- ch
			return nil
		},
	).Times(1)
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()

	// All hosts remain in the ring; nothing should be evicted.
	resolver.EXPECT().Members().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress("addr1:7235"),
	}).AnyTimes()

	pool := NewConnectionPool(
		resolver,
		&fakeRPCFactory{},
		historyservice.NewHistoryServiceClient,
		log.NewNoopLogger(),
	)
	defer pool.stop()

	cc1 := pool.getOrCreateClientConn("addr1:7235")

	notifyCh := <-listenerCh
	notifyCh <- &membership.ChangedEvent{}

	// Give the event loop time to process; addr1 should still be cached.
	time.Sleep(100 * time.Millisecond)

	pool.mu.RLock()
	cached, ok := pool.mu.conns["addr1:7235"]
	pool.mu.RUnlock()
	require.True(t, ok)
	require.Equal(t, cc1.grpcConn, cached.grpcConn)
	require.NotEqual(t, connectivity.Shutdown, cc1.grpcConn.GetState())
}
