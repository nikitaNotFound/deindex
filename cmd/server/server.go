package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nikitaNotFound/deindex/engine"
	"github.com/nikitaNotFound/deindex/evm"
	"github.com/nikitaNotFound/deindex/nodecon"
	"github.com/spf13/cobra"
)

var ServerCmd = &cobra.Command{
	Use:   "server",
	Short: "",
}

var RunServerCmd = &cobra.Command{
	Use:   "run",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		cfg, err := NewServerCfg()
		if err != nil {
			log.Fatalf("failed to create server config: %v", err)
		}

		if err := runServer(cfg); err != nil {
			log.Fatalf("failed to run server: %v", err)
		}
	},
}

func init() {
	ServerCmd.AddCommand(RunServerCmd)
}

func runServer(cfg *ServerCfg) error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-sigCh
		cancel()
	}()

	nodesPool, err := nodecon.CreateNodesPool(
		nodecon.WithRpcProviders(cfg.RpcProviders...),
		nodecon.WithSubProviders(cfg.SubRpcProviders...),
	)
	if err != nil {
		return fmt.Errorf("create nodes pool: %w", err)
	}

	// TODO: replace with actual persistence DB implementation
	e := engine.NewEngine(nil)

	blocksSub := evm.NewBlocksSub(cfg.Network, nodesPool)
	blockSubActor := engine.CreateProducerActor("evm.blocks-sub", blocksSub)

	e.RegisterActors(blockSubActor)

	if err := e.Start(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	return nil
}
