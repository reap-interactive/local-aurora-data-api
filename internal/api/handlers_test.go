package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/reap-interactive/local-aurora-data-api/internal/dataapi"
)

// testHandler builds a Handler with no real database connection — sufficient
// for unit tests that exercise routing, ARN validation, and error formatting.
func testHandler() *Handler {
	return NewHandler(nil, dataapi.NewTransactionStore())
}

// post sends a POST request to path with a JSON body and returns the recorded response.
func post(t *testing.T, mux http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

// decodeError decodes an ErrorResponse from the recorder body.
func decodeError(t *testing.T, w *httptest.ResponseRecorder) ErrorResponse {
	t.Helper()
	var resp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return resp
}

// ── /ExecuteSql ───────────────────────────────────────────────────────────────

func TestHandler_ExecuteSql_NotImplemented(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/ExecuteSql", map[string]string{"sql": "SELECT 1"})

	if w.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", w.Code)
	}
	resp := decodeError(t, w)
	if resp.Code != ErrNotFound {
		t.Errorf("code: got %q, want NotFoundException", resp.Code)
	}
}

// ── /BeginTransaction ─────────────────────────────────────────────────────────

func TestHandler_BeginTransaction_MalformedBody(t *testing.T) {
	mux := NewServer(testHandler())
	req := httptest.NewRequest(http.MethodPost, "/BeginTransaction", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ── /CommitTransaction ────────────────────────────────────────────────────────

func TestHandler_CommitTransaction_UnknownTransactionId(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/CommitTransaction", dataapi.CommitTransactionRequest{
		TransactionID: "does-not-exist",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
	resp := decodeError(t, w)
	if resp.Code != ErrBadRequest {
		t.Errorf("code: got %q, want BadRequestException", resp.Code)
	}
}

// ── /RollbackTransaction ──────────────────────────────────────────────────────

func TestHandler_RollbackTransaction_UnknownTransactionId(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/RollbackTransaction", dataapi.RollbackTransactionRequest{
		TransactionID: "does-not-exist",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ── /Execute ──────────────────────────────────────────────────────────────────

func TestHandler_Execute_UnknownTransactionId(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/Execute", dataapi.ExecuteStatementRequest{
		SQL:           "SELECT 1",
		TransactionID: "ghost-transaction",
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
	resp := decodeError(t, w)
	if resp.Code != ErrBadRequest {
		t.Errorf("code: got %q, want BadRequestException", resp.Code)
	}
}

func TestHandler_Execute_MalformedBody(t *testing.T) {
	mux := NewServer(testHandler())
	req := httptest.NewRequest(http.MethodPost, "/Execute", bytes.NewReader([]byte("{bad")))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ── /BatchExecute ─────────────────────────────────────────────────────────────

func TestHandler_BatchExecute_UnknownTransactionId(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/BatchExecute", dataapi.BatchExecuteStatementRequest{
		SQL:           "INSERT INTO t VALUES (:v)",
		TransactionID: "no-such-tx",
		ParameterSets: [][]dataapi.SQLParameter{
			{{Name: "v", Value: dataapi.LongField(1)}},
		},
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

// ── response format ───────────────────────────────────────────────────────────

func TestHandler_ErrorResponseShape(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/ExecuteSql", nil)

	var m map[string]any
	if err := json.NewDecoder(w.Body).Decode(&m); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := m["message"]; !ok {
		t.Error("missing 'message' field in error response")
	}
	if _, ok := m["code"]; !ok {
		t.Error("missing 'code' field in error response")
	}
}

func TestHandler_ContentTypeIsJSON(t *testing.T) {
	mux := NewServer(testHandler())
	w := post(t, mux, "/ExecuteSql", nil)

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type: got %q, want application/json", ct)
	}
}

// ── HTML escaping ───────────────────────────────────────────────────────────────

// TestWriteJSON_NoHTMLEscaping verifies that writeJSON never replaces <, >, or
// & with their \uXXXX unicode escape sequences. Go's json.Encoder does this by
// default to guard against XSS in HTML contexts, but this server emits
// application/json consumed by AWS SDK clients that expect literal characters.
func TestWriteJSON_NoHTMLEscaping(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, http.StatusOK, map[string]string{"msg": "<hello> & world"})

	body := w.Body.String()
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Errorf("response body contains HTML escape %q: %s", escaped, body)
		}
	}
	if !strings.Contains(body, "<hello> & world") {
		t.Errorf("response body missing raw characters: %s", body)
	}
}

// TestWriteJSON_FormattedRecordsNotEscaped specifically tests the formattedRecords
// field, which embeds a JSON array as a string value. Any <, >, or & inside that
// embedded JSON must survive the outer encoding untouched so the caller can
// unmarshal the string directly.
func TestWriteJSON_FormattedRecordsNotEscaped(t *testing.T) {
	w := httptest.NewRecorder()
	resp := dataapi.ExecuteStatementResponse{
		FormattedRecords: `[{"name":"<test>","filter":"a & b"}]`,
	}
	writeJSON(w, http.StatusOK, resp)

	body := w.Body.String()
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Errorf("formattedRecords was HTML-escaped (%q found): %s", escaped, body)
		}
	}
	// The literal embedded JSON must be present verbatim in the output.
	if !strings.Contains(body, `<test>`) || !strings.Contains(body, `a & b`) {
		t.Errorf("formattedRecords content missing from body: %s", body)
	}
}

// TestWriteError_NoHTMLEscaping verifies that error messages containing HTML
// special characters are also written without escaping.
func TestWriteError_NoHTMLEscaping(t *testing.T) {
	w := httptest.NewRecorder()
	writeError(w, http.StatusBadRequest, ErrBadRequest, `column <id> not found & check schema`)

	body := w.Body.String()
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Errorf("error message was HTML-escaped (%q found): %s", escaped, body)
		}
	}
	if !strings.Contains(body, "<id>") {
		t.Errorf("error message missing raw characters: %s", body)
	}
}
