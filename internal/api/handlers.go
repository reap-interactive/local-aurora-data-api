package api

import (
	"context"
	"encoding/json"
	"log"
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
	log.Printf("[query] %s", req.SQL)

	if strings.TrimSpace(req.TransactionID) != "" {
		tx, err := h.txStore.Get(req.TransactionID)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		resp, err := dataapi.Execute(ctx, tx, &req)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		logExecuteResponse(resp)
		return resp, nil
	}

	resp, err := dataapi.Execute(ctx, h.db, &req)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	logExecuteResponse(resp)
	return resp, nil
}

func (h *Handler) batchExecute(ctx context.Context, req dataapi.BatchExecuteStatementRequest) (any, error) {
	log.Printf("[query] %s", req.SQL)

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
		logBatchResponse(resp)
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

	logBatchResponse(resp)
	return resp, nil
}

// ── logging helpers ───────────────────────────────────────────────────────────

// logExecuteResponse logs the full response body when the result set contains
// fewer than 20 rows — enough to be useful in development without flooding the
// log. Larger result sets emit a one-line summary instead.
func logExecuteResponse(resp *dataapi.ExecuteStatementResponse) {
	var count int
	if resp.FormattedRecords != "" {
		// FormattedRecords is a JSON array string; unmarshal just to count rows.
		var rows []json.RawMessage
		if err := json.Unmarshal([]byte(resp.FormattedRecords), &rows); err == nil {
			count = len(rows)
		}
	} else {
		count = len(resp.Records)
	}

	if count >= 20 {
		log.Printf("[response] %d records (omitted)", count)
		return
	}

	b, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[response] (marshal error: %v)", err)
		return
	}
	log.Printf("[response] %s", b)
}

// logBatchResponse logs the full batch response. Batch results only carry
// generated-field slices (never row data), so they are always compact.
func logBatchResponse(resp *dataapi.BatchExecuteStatementResponse) {
	b, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[response] (marshal error: %v)", err)
		return
	}
	log.Printf("[response] %s", b)
}
