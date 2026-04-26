package dataapi

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"sync"
)

// TransactionStore holds in-flight database transactions keyed by their ID.
// A transaction is created by BeginTransaction and lives until CommitTransaction
// or RollbackTransaction is called.
type TransactionStore struct {
	mu   sync.Mutex
	txns map[string]*sql.Tx
}

// NewTransactionStore returns an empty TransactionStore.
func NewTransactionStore() *TransactionStore {
	return &TransactionStore{txns: make(map[string]*sql.Tx)}
}

// Begin starts a new transaction on db, stores it, and returns its ID.
func (ts *TransactionStore) Begin(db *sql.DB) (string, error) {
	tx, err := db.Begin()
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}

	id, err := generateTransactionID()
	if err != nil {
		_ = tx.Rollback()
		return "", fmt.Errorf("failed to generate transaction ID: %w", err)
	}

	ts.mu.Lock()
	ts.txns[id] = tx
	ts.mu.Unlock()

	return id, nil
}

// Get retrieves the transaction for the given ID without removing it.
func (ts *TransactionStore) Get(id string) (*sql.Tx, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	tx, ok := ts.txns[id]
	if !ok {
		return nil, fmt.Errorf("invalid transaction ID: %s", id)
	}
	return tx, nil
}

// Commit commits the transaction and removes it from the store.
func (ts *TransactionStore) Commit(id string) error {
	ts.mu.Lock()
	tx, ok := ts.txns[id]
	if !ok {
		ts.mu.Unlock()
		return fmt.Errorf("invalid transaction ID: %s", id)
	}
	delete(ts.txns, id)
	ts.mu.Unlock()
	return tx.Commit()
}

// Rollback rolls back the transaction and removes it from the store.
func (ts *TransactionStore) Rollback(id string) error {
	ts.mu.Lock()
	tx, ok := ts.txns[id]
	if !ok {
		ts.mu.Unlock()
		return fmt.Errorf("invalid transaction ID: %s", id)
	}
	delete(ts.txns, id)
	ts.mu.Unlock()
	return tx.Rollback()
}

// generateTransactionID produces a 184-character random base64 string,
// matching the ID length used by the reference implementations.
func generateTransactionID() (string, error) {
	// 138 raw bytes → 184 base64 characters
	b := make([]byte, 138)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
