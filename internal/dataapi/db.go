package dataapi

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// OpenPostgresDB creates a new *pgxpool.Pool connection pool for the given
// PostgreSQL host/port/user/password/dbName combination.
//
// The connection string includes "sslmode=disable" for local development.
func OpenPostgresDB(host, port, user, password, dbName string) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName,
	)

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool.Ping: %w", err)
	}

	return pool, nil
}
