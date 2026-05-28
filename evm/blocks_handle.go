package evm

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/nikitaNotFound/deindex/engine"
	"github.com/nikitaNotFound/deindex/messages"
	"github.com/nikitaNotFound/deindex/nodecon"
	"github.com/nikitaNotFound/deindex/types"
)

type BlocksHandle struct {
	network   types.Network
	nodesPool *nodecon.NodesPool
}

func NewBlocksHandle(network types.Network, nodesPool *nodecon.NodesPool) *BlocksHandle {
	return &BlocksHandle{
		network:   network,
		nodesPool: nodesPool,
	}
}

func (b *BlocksHandle) Receive(engineCtx engine.EngineCtx, msg *messages.BlockMessage) error {
	ethClient := ethclient.NewClient(b.nodesPool.GetActiveRpcProvider())
	block, err := ethClient.BlockByHash(engineCtx.Context(), msg.Header.Hash())
	if err != nil {
		return fmt.Errorf("get block by hash: %w", err)
	}

	for _, tx := range block.Transactions() {
		if err := engineCtx.Publish(engineCtx.Context(), &messages.RawTransactionMessage{Transaction: tx}); err != nil {
			return fmt.Errorf("publish raw transaction message: %w", err)
		}
	}

	return nil
}
