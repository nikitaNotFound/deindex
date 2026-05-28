package evm

import (
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nikitaNotFound/deindex/engine"
	"github.com/nikitaNotFound/deindex/messages"
)

type TxHandler struct {
}

func NewTxHandler() *TxHandler {
	return &TxHandler{}
}

func (t *TxHandler) Receive(engineCtx engine.EngineCtx, msg engine.Message) error {
	switch msg.Topic() {
	case messages.TopicRawTransaction:
		txMsg, ok := msg.Args().(*messages.RawTransactionMessage)
		if !ok {
			return fmt.Errorf("invalid raw transaction message: %T", msg.Args())
		}
		return t.handleRawTransaction(engineCtx, txMsg.Transaction)
	}

	return nil
}

func (t *TxHandler) handleRawTransaction(engineCtx engine.EngineCtx, transaction *ethtypes.Transaction) error {
	return nil
}
