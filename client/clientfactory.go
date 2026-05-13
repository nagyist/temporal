//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination client_factory_mock.go

package client

import (
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client/admin"
	"go.temporal.io/server/client/frontend"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

type (
	// Factory can be used to create RPC clients for temporal services
	Factory interface {
		NewHistoryClientWithTimeout(timeout time.Duration) (historyservice.HistoryServiceClient, error)
		NewMatchingClientWithTimeout(namespaceIDToName NamespaceIDToNameFunc, timeout time.Duration, longPollTimeout time.Duration) (matchingservice.MatchingServiceClient, error)
		NewRemoteFrontendClientWithTimeout(rpcAddress string, timeout time.Duration, longPollTimeout time.Duration) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient)
		NewLocalFrontendClientWithTimeout(timeout time.Duration, longPollTimeout time.Duration) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error)
		NewRemoteAdminClientWithTimeout(rpcAddress string, timeout time.Duration, largeTimeout time.Duration) adminservice.AdminServiceClient
		NewLocalAdminClientWithTimeout(timeout time.Duration, largeTimeout time.Duration) (adminservice.AdminServiceClient, error)
	}

	// FactoryProvider can be used to provide a customized client Factory implementation.
	FactoryProvider interface {
		NewFactory(
			rpcFactory common.RPCFactory,
			monitor membership.Monitor,
			metricsHandler metrics.Handler,
			dc *dynamicconfig.Collection,
			testHooks testhooks.TestHooks,
			numberOfHistoryShards int32,
			logger log.Logger,
			throttledLogger log.Logger,
		) Factory
	}

	// NamespaceIDToNameFunc maps a namespaceID to namespace name. Returns error when mapping is not possible.
	NamespaceIDToNameFunc func(id namespace.ID) (namespace.Name, error)

	rpcClientFactory struct {
		rpcFactory            common.RPCFactory
		monitor               membership.Monitor
		metricsHandler        metrics.Handler
		dynConfig             *dynamicconfig.Collection
		testHooks             testhooks.TestHooks
		numberOfHistoryShards int32
		logger                log.Logger
		throttledLogger       log.Logger
	}

	factoryProviderImpl struct {
	}

	serviceKeyResolverImpl struct {
		resolver membership.ServiceResolver
	}
)

// NewFactoryProvider creates a default implementation of FactoryProvider.
func NewFactoryProvider() FactoryProvider {
	return &factoryProviderImpl{}
}

// NewFactory creates an instance of client factory that knows how to dispatch RPC calls.
func (p *factoryProviderImpl) NewFactory(
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	metricsHandler metrics.Handler,
	dc *dynamicconfig.Collection,
	testHooks testhooks.TestHooks,
	numberOfHistoryShards int32,
	logger log.Logger,
	throttledLogger log.Logger,
) Factory {
	return &rpcClientFactory{
		rpcFactory:            rpcFactory,
		monitor:               monitor,
		metricsHandler:        metricsHandler,
		dynConfig:             dc,
		testHooks:             testHooks,
		numberOfHistoryShards: numberOfHistoryShards,
		logger:                logger,
		throttledLogger:       throttledLogger,
	}
}

func (cf *rpcClientFactory) NewHistoryClientWithTimeout(timeout time.Duration) (historyservice.HistoryServiceClient, error) {
	resolver, err := cf.monitor.GetResolver(primitives.HistoryService)
	if err != nil {
		return nil, err
	}
	client := history.NewClient(
		cf.dynConfig,
		resolver,
		cf.logger,
		cf.numberOfHistoryShards,
		cf.rpcFactory,
		timeout,
	)
	if cf.metricsHandler != nil {
		client = history.NewMetricClient(client, cf.metricsHandler, cf.logger, cf.throttledLogger)
	}
	return client, nil
}

func (cf *rpcClientFactory) NewMatchingClientWithTimeout(
	namespaceIDToName NamespaceIDToNameFunc,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (matchingservice.MatchingServiceClient, error) {
	resolver, err := cf.monitor.GetResolver(primitives.MatchingService)
	if err != nil {
		return nil, err
	}

	keyResolver := newServiceKeyResolver(resolver)
	clientProvider := func(clientKey string) (any, io.Closer, error) {
		connection := cf.rpcFactory.CreateMatchingGRPCConnection(clientKey)
		return matchingservice.NewMatchingServiceClient(connection), connection, nil
	}
	membershipSub := common.MembershipSubscription{
		Subscribe: func(notifyCh chan<- struct{}) (func(), error) {
			listenerName := fmt.Sprintf("matchingClientCache-%s", uuid.New().String())
			internalCh := make(chan *membership.ChangedEvent, 1)
			if err := resolver.AddListener(listenerName, internalCh); err != nil {
				return nil, err
			}
			done := make(chan struct{})
			go func() {
				for {
					select {
					case <-done:
						return
					case <-internalCh:
						select {
						case notifyCh <- struct{}{}:
						default:
						}
					}
				}
			}()
			return func() {
				close(done)
				_ = resolver.RemoveListener(listenerName)
			}, nil
		},
		CurrentAddresses: func() []string {
			members := resolver.Members()
			addrs := make([]string, 0, len(members))
			for _, m := range members {
				addrs = append(addrs, m.GetAddress())
			}
			return addrs
		},
	}
	client := matching.NewClient(
		timeout,
		longPollTimeout,
		common.NewClientCache(keyResolver, clientProvider, membershipSub, cf.logger),
		cf.metricsHandler,
		cf.logger,
		matching.NewLoadBalancer(namespaceIDToName, cf.dynConfig, cf.testHooks),
		dynamicconfig.MatchingSpreadRoutingBatchSize.Get(cf.dynConfig),
	)

	if cf.metricsHandler != nil {
		client = matching.NewMetricClient(client, cf.metricsHandler, cf.logger, cf.throttledLogger)
	}
	return client, nil

}

func (cf *rpcClientFactory) NewRemoteFrontendClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient) {
	connection := cf.rpcFactory.CreateRemoteFrontendGRPCConnection(rpcAddress)
	client := workflowservice.NewWorkflowServiceClient(connection)
	return connection, cf.newFrontendClient(client, timeout, longPollTimeout)
}

func (cf *rpcClientFactory) NewLocalFrontendClientWithTimeout(
	timeout time.Duration,
	longPollTimeout time.Duration,
) (grpc.ClientConnInterface, workflowservice.WorkflowServiceClient, error) {
	connection := cf.rpcFactory.CreateLocalFrontendGRPCConnection()
	client := workflowservice.NewWorkflowServiceClient(connection)
	return connection, cf.newFrontendClient(client, timeout, longPollTimeout), nil
}

func (cf *rpcClientFactory) NewRemoteAdminClientWithTimeout(
	rpcAddress string,
	timeout time.Duration,
	largeTimeout time.Duration,
) adminservice.AdminServiceClient {
	connection := cf.rpcFactory.CreateRemoteFrontendGRPCConnection(rpcAddress)
	client := adminservice.NewAdminServiceClient(connection)
	return cf.newAdminClient(client, timeout, largeTimeout)
}

func (cf *rpcClientFactory) NewLocalAdminClientWithTimeout(
	timeout time.Duration,
	longPollTimeout time.Duration,
) (adminservice.AdminServiceClient, error) {
	connection := cf.rpcFactory.CreateLocalFrontendGRPCConnection()
	client := adminservice.NewAdminServiceClient(connection)
	return cf.newAdminClient(client, timeout, longPollTimeout), nil
}

func (cf *rpcClientFactory) newAdminClient(
	client adminservice.AdminServiceClient,
	timeout time.Duration,
	longPollTimeout time.Duration,
) adminservice.AdminServiceClient {
	client = admin.NewClient(timeout, longPollTimeout, client)
	if cf.metricsHandler != nil {
		client = admin.NewMetricClient(client, cf.metricsHandler, cf.throttledLogger)
	}
	return client
}

func (cf *rpcClientFactory) newFrontendClient(
	client workflowservice.WorkflowServiceClient,
	timeout time.Duration,
	longPollTimeout time.Duration,
) workflowservice.WorkflowServiceClient {
	client = frontend.NewClient(timeout, longPollTimeout, client)
	if cf.metricsHandler != nil {
		client = frontend.NewMetricClient(client, cf.metricsHandler, cf.throttledLogger)
	}
	return client
}

func newServiceKeyResolver(resolver membership.ServiceResolver) *serviceKeyResolverImpl {
	return &serviceKeyResolverImpl{
		resolver: resolver,
	}
}

// Lookup returns the address for a node within a batch. key contains the key (including batch
// number), and index is the index within the batch. If not using batches, index should be 0.
// Note that Lookup(key) and LookupN(key, n)[0] are equal.
func (r *serviceKeyResolverImpl) Lookup(key string, index int) (string, error) {
	hosts := r.resolver.LookupN(key, index+1)
	if len(hosts) == 0 {
		return "", membership.ErrInsufficientHosts
	}
	if index >= len(hosts) {
		index %= len(hosts)
	}
	return hosts[index].GetAddress(), nil
}

func (r *serviceKeyResolverImpl) GetAllAddresses() ([]string, error) {
	var all []string

	for _, host := range r.resolver.Members() {
		all = append(all, host.GetAddress())
	}

	return all, nil
}
