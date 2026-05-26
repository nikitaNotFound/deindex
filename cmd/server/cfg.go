package server

import (
	"errors"
	"fmt"

	"github.com/caarlos0/env/v11"
	_ "github.com/joho/godotenv/autoload"
	"github.com/nikitaNotFound/deindex/types"
)

type ServerCfg struct {
	Network         types.Network `env:"NETWORK"`
	RpcProviders    []string      `env:"RPC_PROVIDERS" envSeparator:","`
	SubRpcProviders []string      `env:"SUB_RPC_PROVIDERS" envSeparator:","`
}

func NewServerCfg() (*ServerCfg, error) {
	cfg := &ServerCfg{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing server config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating server config: %w", err)
	}

	return cfg, nil
}

func (cfg *ServerCfg) Validate() error {
	if cfg.Network == "" {
		return errors.New("network is required")
	}

	if len(cfg.SubRpcProviders) == 0 {
		return errors.New("sub rpc providers are required")
	}

	return nil
}
