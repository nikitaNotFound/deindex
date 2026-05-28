package evm

import (
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nikitaNotFound/deindex/engine"
	"github.com/nikitaNotFound/deindex/messages"
)

type TxHandler struct {
}

func NewTxHandler() *TxHandler {
	return &TxHandler{}
}

func (t *TxHandler) Receive(engineCtx engine.EngineCtx, msg *messages.RawTransactionMessage) error {
	return t.handleRawTransaction(engineCtx, msg.Transaction)
}

func (t *TxHandler) handleRawTransaction(engineCtx engine.EngineCtx, transaction *ethtypes.Transaction) error {
	return nil
}
