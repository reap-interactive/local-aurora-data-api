package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/reap-interactive/local-aurora-data-api/internal/dataapi"
)

// Handler holds the shared state needed to serve all Data API endpoints.
type Handler struct {
	db      *pgxpool.Pool
	txStore *dataapi.TransactionStore
}

// NewHandler creates a Handler wired to the given database and transaction store.
func NewHandler(db *pgxpool.Pool, txStore *dataapi.TransactionStore) *Handler {
	return &Handler{db: db, txStore: txStore}
}

// ── handlers ──────────────────────────────────────────────────────────────────

// executeSQL handles POST /ExecuteSql.
// The legacy ExecuteSql operation is not implemented.
func (h *Handler) executeSQL(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, ErrNotFound, "NotImplemented")
}

func (h *Handler) beginTransaction(ctx context.Context, req dataapi.BeginTransactionRequest) (any, error) {
	txID, err := h.txStore.Begin(ctx, h.db)
	if err != nil {
		return nil, internalError(err.Error())
	}
	return dataapi.BeginTransactionResponse{TransactionID: txID}, nil
}

func (h *Handler) commitTransaction(ctx context.Context, req dataapi.CommitTransactionRequest) (any, error) {
	if err := h.txStore.Commit(ctx, req.TransactionID); err != nil {
		return nil, badRequest(err.Error())
	}
	return dataapi.CommitTransactionResponse{TransactionStatus: "Transaction Committed"}, nil
}

func (h *Handler) rollbackTransaction(ctx context.Context, req dataapi.RollbackTransactionRequest) (any, error) {
	if err := h.txStore.Rollback(ctx, req.TransactionID); err != nil {
		return nil, badRequest(err.Error())
	}
	return dataapi.RollbackTransactionResponse{TransactionStatus: "Rollback Complete"}, nil
}

func (h *Handler) execute(ctx context.Context, req dataapi.ExecuteStatementRequest) (any, error) {
	if strings.TrimSpace(req.TransactionID) != "" {
		tx, err := h.txStore.Get(req.TransactionID)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		resp, err := dataapi.Execute(ctx, tx, &req)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		return resp, nil
	}

	resp, err := dataapi.Execute(ctx, h.db, &req)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	return resp, nil
}

func (h *Handler) batchExecute(ctx context.Context, req dataapi.BatchExecuteStatementRequest) (any, error) {
	if req.ParameterSets == nil {
		req.ParameterSets = [][]dataapi.SQLParameter{}
	}

	if strings.TrimSpace(req.TransactionID) != "" {
		tx, err := h.txStore.Get(req.TransactionID)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		resp, err := dataapi.BatchExecute(ctx, tx, &req)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		return resp, nil
	}

	// Auto-commit: wrap the whole batch in a single transaction so either all
	// rows are inserted or none are.
	tx, err := h.db.Begin(ctx)
	if err != nil {
		return nil, internalError(err.Error())
	}

	resp, err := dataapi.BatchExecute(ctx, tx, &req)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, badRequest(err.Error())
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, internalError(err.Error())
	}

	return resp, nil
}
