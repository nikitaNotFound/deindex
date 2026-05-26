package pgstorage

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikitaNotFound/deindex/pgstorage/sqlcgen"
)

type Storage struct {
	Queries *sqlcgen.Queries
	pool    *pgxpool.Pool
}

func NewStorage(ctx context.Context, pool *pgxpool.Pool) (*Storage, error) {
	return &Storage{
		Queries: sqlcgen.New(pool),
		pool:    pool,
	}, nil
}

func (s *Storage) GetPool() *pgxpool.Pool {
	return s.pool
}

func (s *Storage) BeginTx(ctx context.Context) (pgx.Tx, sqlcgen.Querier, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	qtx := s.Queries.WithTx(tx)

	return tx, qtx, nil
}

func (s *Storage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
