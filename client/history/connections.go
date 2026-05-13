package history

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
)

type (
	clientConnection[C any] struct {
		grpcClient C
		grpcConn   *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl[C any] struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection[C]
		}

		historyServiceResolver membership.ServiceResolver
		rpcFactory             RPCFactory
		clientCtor             func(grpc.ClientConnInterface) C
		logger                 log.Logger
		cancelEventLoop        context.CancelFunc
	}

	// RPCFactory is a subset of the [go.temporal.io/server/common/rpc.RPCFactory] interface to make testing easier.
	RPCFactory interface {
		CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn
	}

	connectionPool[C any] interface {
		getOrCreateClientConn(addr rpcAddress) clientConnection[C]
		getAllClientConns() []clientConnection[C]
		resetConnectBackoff(clientConnection[C])
	}
)

func NewConnectionPool[C any](
	historyServiceResolver membership.ServiceResolver,
	rpcFactory RPCFactory,
	clientCtor func(grpc.ClientConnInterface) C,
	logger log.Logger,
) *connectionPoolImpl[C] {
	c := &connectionPoolImpl[C]{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection[C])
	ctx, cancel := context.WithCancel(context.Background())
	c.cancelEventLoop = cancel
	go c.eventLoop(ctx)
	return c
}

func (c *connectionPoolImpl[C]) getOrCreateClientConn(addr rpcAddress) clientConnection[C] {
	c.mu.RLock()
	cc, ok := c.mu.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.mu.conns[addr]; ok {
		return cc
	}
	grpcConn := c.rpcFactory.CreateHistoryGRPCConnection(string(addr))
	cc = clientConnection[C]{
		grpcClient: c.clientCtor(grpcConn),
		grpcConn:   grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *connectionPoolImpl[C]) getAllClientConns() []clientConnection[C] {
	hostInfos := c.historyServiceResolver.Members()

	var clientConns []clientConnection[C]
	for _, hostInfo := range hostInfos {
		cc := c.getOrCreateClientConn(rpcAddress(hostInfo.GetAddress()))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}

func (c *connectionPoolImpl[C]) resetConnectBackoff(cc clientConnection[C]) {
	cc.grpcConn.ResetConnectBackoff()
}

func (c *connectionPoolImpl[C]) eventLoop(ctx context.Context) {
	listenerName := fmt.Sprintf("connectionPoolListener-%s", uuid.New().String())
	updateCh := make(chan *membership.ChangedEvent, 1)
	if err := c.historyServiceResolver.AddListener(listenerName, updateCh); err != nil {
		c.logger.Error("Error adding membership listener", tag.Error(err))
		return
	}
	defer func() {
		if err := c.historyServiceResolver.RemoveListener(listenerName); err != nil {
			c.logger.Warn("Error removing membership listener", tag.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-updateCh:
			c.evictStale()
		}
	}
}

// evictStale closes connections for hosts no longer in the ring. In-flight
// RPCs fail fast with Unavailable; the redirector retries.
func (c *connectionPoolImpl[C]) evictStale() {
	members := c.historyServiceResolver.Members()
	live := make(map[rpcAddress]struct{}, len(members))
	for _, m := range members {
		live[rpcAddress(m.GetAddress())] = struct{}{}
	}

	var toClose []*grpc.ClientConn
	c.mu.Lock()
	for addr, cc := range c.mu.conns {
		if _, ok := live[addr]; !ok {
			toClose = append(toClose, cc.grpcConn)
			delete(c.mu.conns, addr)
		}
	}
	c.mu.Unlock()

	for _, conn := range toClose {
		if err := conn.Close(); err != nil {
			c.logger.Warn("Error closing evicted gRPC connection", tag.Error(err))
		}
	}
}

func (c *connectionPoolImpl[C]) stop() {
	c.cancelEventLoop()
}
