package subscribers

import (
	"context"
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/nikitaNotFound/deindex/nodecon"
	"github.com/nikitaNotFound/deindex/types"
)

type BlocksSubscriber struct {
	network   types.Network
	nodesPool *nodecon.NodesPool

	blockHandler *blockHandler
}

func NewBlocksSubscriber(network types.Network, nodesPool *nodecon.NodesPool) *BlocksSubscriber {
	return &BlocksSubscriber{
		network:   network,
		nodesPool: nodesPool,

		blockHandler: NewBlockHandler(network),
	}
}

func (b *BlocksSubscriber) Start(ctx context.Context) error {
	client := ethclient.NewClient(b.nodesPool.GetActiveSubProvider())

	headers := make(chan *ethtypes.Header)
	sub, err := client.SubscribeNewHead(ctx, headers)
	if err != nil {
		return fmt.Errorf("subscribe new head: %w", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sub.Err():
			return fmt.Errorf("subscription error on %s: %w", b.network, err)
		case header := <-headers:
			if err := b.blockHandler.handleBlock(ctx, header); err != nil {
				return fmt.Errorf("handle block: %w", err)
			}
		}
	}
}
