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

func (b *BlocksHandle) Receive(engineCtx engine.EngineCtx, msg engine.Message) error {
	switch msg.Topic() {
	case messages.TopicBlock:
		blockMsg, ok := msg.Payload().(*messages.BlockMessage)
		if !ok {
			return fmt.Errorf("invalid block message: %T", msg.Payload())
		}

		return b.handleBlock(engineCtx, blockMsg.Header)
	default:
		return fmt.Errorf("unknown topic: %s", msg.Topic())
	}
}

func (b *BlocksHandle) handleBlock(engineCtx engine.EngineCtx, header *ethtypes.Header) error {
	ethClient := ethclient.NewClient(b.nodesPool.GetActiveRpcProvider())
	block, err := ethClient.BlockByHash(engineCtx.Context(), header.Hash())
	if err != nil {
		return fmt.Errorf("get block by hash: %w", err)
	}

	txs := block.Transactions()
	txMessages := make([]engine.Message, 0, len(txs))
	for _, tx := range txs {
		txMessages = append(txMessages, messages.NewRawTransactionMessage(tx))
	}

	for _, txMessage := range txMessages {
		if err := engineCtx.PublishMsg(engineCtx.Context(), txMessage); err != nil {
			return fmt.Errorf("publish raw transaction message: %w", err)
		}
	}

	return nil
}
