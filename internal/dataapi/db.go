package dataapi

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// OpenPostgresDB creates a new *sql.DB connection pool for the given PostgreSQL
// host/port/user/password/dbName combination using the pgx/v5 stdlib driver.
//
// The connection string includes "sslmode=disable" for local development.
func OpenPostgresDB(host, port, user, password, dbName string) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbName,
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("db.Ping: %w", err)
	}

	return db, nil
}
