package nodecon

import "github.com/ethereum/go-ethereum/rpc"

type NodesPoolOpt func(*NodesPool)

func WithRpcProviders(rpcProviders ...string) NodesPoolOpt {
	return func(pool *NodesPool) {
		pool.rpcProviderUrls = rpcProviders
	}
}

func WithSubProviders(subProviders ...string) NodesPoolOpt {
	return func(pool *NodesPool) {
		pool.subRpcProviderUrls = subProviders
	}
}

type NodesPool struct {
	rpcProviderUrls    []string
	subRpcProviderUrls []string

	rpcProviders    []*rpc.Client
	subRpcProviders []*rpc.Client

	activeRpcProviderIndex int
	activeSubProviderIndex int
}

func CreateNodesPool(opts ...NodesPoolOpt) (*NodesPool, error) {
	pool := &NodesPool{}
	for _, opt := range opts {
		opt(pool)
	}

	rpcProviders := make([]*rpc.Client, len(pool.rpcProviderUrls))
	for _, url := range pool.rpcProviderUrls {
		client, err := rpc.Dial(url)
		if err != nil {
			return nil, err
		}
		rpcProviders = append(pool.rpcProviders, client)
	}

	subRpcProviders := make([]*rpc.Client, len(pool.subRpcProviderUrls))
	for _, url := range pool.subRpcProviderUrls {
		if err := ValidateSubRpcUrl(url); err != nil {
			return nil, err
		}

		client, err := rpc.Dial(url)
		if err != nil {
			return nil, err
		}
		subRpcProviders = append(pool.subRpcProviders, client)
	}

	pool.rpcProviders = rpcProviders
	pool.subRpcProviders = subRpcProviders

	return pool, nil
}

func (np *NodesPool) GetActiveRpcProvider() *rpc.Client {
	return np.rpcProviders[np.activeRpcProviderIndex]
}

func (np *NodesPool) GetActiveSubProvider() *rpc.Client {
	return np.subRpcProviders[np.activeSubProviderIndex]
}
