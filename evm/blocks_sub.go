package evm

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/nikitaNotFound/deindex/engine"
	"github.com/nikitaNotFound/deindex/messages"
	"github.com/nikitaNotFound/deindex/nodecon"
	"github.com/nikitaNotFound/deindex/types"
)

type BlocksSub struct {
	network   types.Network
	nodesPool *nodecon.NodesPool
}

func NewBlocksSub(network types.Network, nodesPool *nodecon.NodesPool) *BlocksSub {
	return &BlocksSub{
		network:   network,
		nodesPool: nodesPool,
	}
}

func (b *BlocksSub) Start(engineCtx engine.EngineCtx) error {
	ctx := engineCtx.Context()

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
			if err := engineCtx.Publish(ctx, messages.NewBlockMessage(header)); err != nil {
				return fmt.Errorf("handle block: %w", err)
			}
		}
	}
}
