package messages

import (
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nikitaNotFound/deindex/engine"
)

type BlockMessage struct {
	Header *ethtypes.Header
}

func NewBlockMessage(header *ethtypes.Header) *BlockMessage {
	return &BlockMessage{
		Header: header,
	}
}

func (m *BlockMessage) Topic() engine.TopicID {
	return TopicBlock
}

func (m *BlockMessage) Args() any {
	return m.Header
}

func (m *BlockMessage) ID() string {
	return m.Header.Hash().Hex()
}

type RawTransactionMessage struct {
	Transaction *ethtypes.Transaction
}

func NewRawTransactionMessage(transaction *ethtypes.Transaction) *RawTransactionMessage {
	return &RawTransactionMessage{
		Transaction: transaction,
	}
}

func (m *RawTransactionMessage) Topic() engine.TopicID {
	return TopicRawTransaction
}

func (m *RawTransactionMessage) Args() any {
	return m.Transaction
}

func (m *RawTransactionMessage) ID() string {
	return m.Transaction.Hash().Hex()
}
