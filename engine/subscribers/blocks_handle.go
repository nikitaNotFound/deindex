package subscribers

import (
	"context"
	"fmt"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/nikitaNotFound/deindex/types"
)

type blockHandler struct {
	network types.Network
}

func NewBlockHandler(network types.Network) *blockHandler {
	return &blockHandler{
		network: network,
	}
}

func (b *blockHandler) handleBlock(ctx context.Context, header *ethtypes.Header) error {
	fmt.Printf("[%s] new block %s %s\n", b.network, header.Number.String(), header.Hash().Hex())
	return nil
}
