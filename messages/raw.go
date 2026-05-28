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
