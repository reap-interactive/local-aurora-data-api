//go:build integration

package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/reap-interactive/local-aurora-data-api/internal/dataapi"
)

// Integration tests require a running PostgreSQL instance.
// Run with: go test -tags integration ./...
//
// The target database is configured via the same environment variables as the
// server binary. Defaults match the docker-compose.yml in this directory.

func getTestEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func integrationHandler(t *testing.T) (*Handler, *httptest.Server) {
	t.Helper()

	db, err := dataapi.OpenPostgresDB(
		getTestEnv("POSTGRES_HOST", "127.0.0.1"),
		getTestEnv("POSTGRES_PORT", "5432"),
		getTestEnv("POSTGRES_USER", "postgres"),
		getTestEnv("POSTGRES_PASSWORD", "example"),
		getTestEnv("POSTGRES_DB", "postgres"),
	)
	if err != nil {
		t.Fatalf("OpenPostgresDB: %v", err)
	}

	h := NewHandler(db, dataapi.NewTransactionStore())
	srv := httptest.NewServer(NewServer(h))
	t.Cleanup(func() {
		srv.Close()
		db.Close()
	})
	return h, srv
}

func testResourceArn() string {
	return getTestEnv("RESOURCE_ARN", "arn:aws:rds:us-east-1:123456789012:cluster:dummy")
}

func testSecretArn() string {
	return getTestEnv("SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123456789012:secret:dummy")
}

// apiPost sends a POST request to path on the test server and decodes the
// response body into dest (if non-nil). Returns the HTTP status code.
func apiPost(t *testing.T, srv *httptest.Server, path string, body any, dest any) int {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if dest != nil {
		if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
			t.Fatalf("decode response from %s: %v", path, err)
		}
	}
	return resp.StatusCode
}

// testTable creates a temporary table, runs fn, then drops the table.
func testTable(t *testing.T, srv *httptest.Server, ddl string, fn func()) {
	t.Helper()
	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn: testResourceArn(),
		SecretArn:   testSecretArn(),
		SQL:         ddl,
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("create table: status %d", status)
	}
	t.Cleanup(func() {
		var name string
		fmt.Sscanf(ddl, "CREATE TABLE %s", &name)
		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "DROP TABLE IF EXISTS " + name,
		}, nil)
	})
	fn()
}

// apiPostBytes sends a POST request to path and returns the HTTP status code
// together with the raw (undecoded) response body bytes. Use this instead of
// apiPost whenever the test needs to inspect the literal wire representation
// (e.g. to assert that HTML characters are not unicode-escaped).
func apiPostBytes(t *testing.T, srv *httptest.Server, path string, body any) (int, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(srv.URL+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body from %s: %v", path, err)
	}
	return resp.StatusCode, raw
}

// ── SELECT ────────────────────────────────────────────────────────────────────

func TestIntegration_SelectOne(t *testing.T) {
	_, srv := integrationHandler(t)

	var resp dataapi.ExecuteStatementResponse
	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn: testResourceArn(),
		SecretArn:   testSecretArn(),
		SQL:         "SELECT 1 AS n",
	}, &resp)

	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if len(resp.Records) != 1 || len(resp.Records[0]) != 1 {
		t.Fatalf("expected 1 record with 1 field, got %+v", resp.Records)
	}
	f := resp.Records[0][0]
	if f.LongValue == nil || *f.LongValue != 1 {
		t.Errorf("expected longValue=1, got %+v", f)
	}
}

func TestIntegration_SelectWithMetadata(t *testing.T) {
	_, srv := integrationHandler(t)

	var resp dataapi.ExecuteStatementResponse
	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn:           testResourceArn(),
		SecretArn:             testSecretArn(),
		SQL:                   "SELECT 42::int4 AS answer",
		IncludeResultMetadata: true,
	}, &resp)

	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if len(resp.ColumnMetadata) != 1 {
		t.Fatalf("expected 1 column metadata, got %d", len(resp.ColumnMetadata))
	}
	if resp.ColumnMetadata[0].Name != "answer" {
		t.Errorf("column name: got %q, want answer", resp.ColumnMetadata[0].Name)
	}
}

func TestIntegration_SelectFormattedRecords(t *testing.T) {
	_, srv := integrationHandler(t)

	var resp dataapi.ExecuteStatementResponse
	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn:     testResourceArn(),
		SecretArn:       testSecretArn(),
		SQL:             "SELECT 1 AS id, 'alice' AS name",
		FormatRecordsAs: "JSON",
	}, &resp)

	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if resp.FormattedRecords == "" {
		t.Fatal("formattedRecords should not be empty")
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(resp.FormattedRecords), &rows); err != nil {
		t.Fatalf("parse formattedRecords: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

// ── INSERT / UPDATE / DELETE ───────────────────────────────────────────────────

func TestIntegration_InsertAndSelect(t *testing.T) {
	_, srv := integrationHandler(t)
	tableName := "test_insert_" + fmt.Sprintf("%d", os.Getpid())

	testTable(t, srv, "CREATE TABLE "+tableName+" (id SERIAL PRIMARY KEY, val TEXT)", func() {
		var insertResp dataapi.ExecuteStatementResponse
		status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "INSERT INTO " + tableName + " (val) VALUES (:val)",
			Parameters:  []dataapi.SQLParameter{{Name: "val", Value: dataapi.StringField("hello")}},
		}, &insertResp)

		if status != http.StatusOK {
			t.Fatalf("insert status: %d", status)
		}
		if insertResp.NumberOfRecordsUpdated != 1 {
			t.Errorf("numberOfRecordsUpdated: got %d, want 1", insertResp.NumberOfRecordsUpdated)
		}

		var selResp dataapi.ExecuteStatementResponse
		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "SELECT val FROM " + tableName,
		}, &selResp)

		if len(selResp.Records) != 1 {
			t.Fatalf("expected 1 record, got %d", len(selResp.Records))
		}
		f := selResp.Records[0][0]
		if f.StringValue == nil || *f.StringValue != "hello" {
			t.Errorf("val: got %+v, want stringValue=hello", f)
		}
	})
}

// ── Transactions ──────────────────────────────────────────────────────────────

func TestIntegration_TransactionCommit(t *testing.T) {
	_, srv := integrationHandler(t)
	tableName := "test_txn_commit_" + fmt.Sprintf("%d", os.Getpid())

	testTable(t, srv, "CREATE TABLE "+tableName+" (id SERIAL, val INT)", func() {
		var beginResp dataapi.BeginTransactionResponse
		if s := apiPost(t, srv, "/BeginTransaction", dataapi.BeginTransactionRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
		}, &beginResp); s != http.StatusOK {
			t.Fatalf("BeginTransaction status: %d", s)
		}
		txID := beginResp.TransactionID
		if txID == "" {
			t.Fatal("empty transactionId")
		}

		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn:   testResourceArn(),
			SecretArn:     testSecretArn(),
			SQL:           "INSERT INTO " + tableName + " (val) VALUES (99)",
			TransactionID: txID,
		}, nil)

		var commitResp dataapi.CommitTransactionResponse
		if s := apiPost(t, srv, "/CommitTransaction", dataapi.CommitTransactionRequest{
			ResourceArn:   testResourceArn(),
			SecretArn:     testSecretArn(),
			TransactionID: txID,
		}, &commitResp); s != http.StatusOK {
			t.Fatalf("CommitTransaction status: %d", s)
		}
		if commitResp.TransactionStatus != "Transaction Committed" {
			t.Errorf("status: got %q", commitResp.TransactionStatus)
		}

		var selResp dataapi.ExecuteStatementResponse
		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "SELECT val FROM " + tableName,
		}, &selResp)

		if len(selResp.Records) != 1 {
			t.Errorf("expected 1 row after commit, got %d", len(selResp.Records))
		}
	})
}

func TestIntegration_TransactionRollback(t *testing.T) {
	_, srv := integrationHandler(t)
	tableName := "test_txn_rollback_" + fmt.Sprintf("%d", os.Getpid())

	testTable(t, srv, "CREATE TABLE "+tableName+" (id SERIAL, val INT)", func() {
		var beginResp dataapi.BeginTransactionResponse
		apiPost(t, srv, "/BeginTransaction", dataapi.BeginTransactionRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
		}, &beginResp)

		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn:   testResourceArn(),
			SecretArn:     testSecretArn(),
			SQL:           "INSERT INTO " + tableName + " (val) VALUES (42)",
			TransactionID: beginResp.TransactionID,
		}, nil)

		var rollbackResp dataapi.RollbackTransactionResponse
		if s := apiPost(t, srv, "/RollbackTransaction", dataapi.RollbackTransactionRequest{
			ResourceArn:   testResourceArn(),
			SecretArn:     testSecretArn(),
			TransactionID: beginResp.TransactionID,
		}, &rollbackResp); s != http.StatusOK {
			t.Fatalf("RollbackTransaction status: %d", s)
		}
		if rollbackResp.TransactionStatus != "Rollback Complete" {
			t.Errorf("status: got %q", rollbackResp.TransactionStatus)
		}

		var selResp dataapi.ExecuteStatementResponse
		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "SELECT val FROM " + tableName,
		}, &selResp)

		if len(selResp.Records) != 0 {
			t.Errorf("expected 0 rows after rollback, got %d", len(selResp.Records))
		}
	})
}

// ── BatchExecute ──────────────────────────────────────────────────────────────

func TestIntegration_BatchExecute(t *testing.T) {
	_, srv := integrationHandler(t)
	tableName := "test_batch_" + fmt.Sprintf("%d", os.Getpid())

	testTable(t, srv, "CREATE TABLE "+tableName+" (id SERIAL, val INT)", func() {
		var batchResp dataapi.BatchExecuteStatementResponse
		status := apiPost(t, srv, "/BatchExecute", dataapi.BatchExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "INSERT INTO " + tableName + " (val) VALUES (:v)",
			ParameterSets: [][]dataapi.SQLParameter{
				{{Name: "v", Value: dataapi.LongField(10)}},
				{{Name: "v", Value: dataapi.LongField(20)}},
				{{Name: "v", Value: dataapi.LongField(30)}},
			},
		}, &batchResp)

		if status != http.StatusOK {
			t.Fatalf("BatchExecute status: %d", status)
		}
		if len(batchResp.UpdateResults) != 3 {
			t.Errorf("expected 3 updateResults, got %d", len(batchResp.UpdateResults))
		}

		var selResp dataapi.ExecuteStatementResponse
		apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
			ResourceArn: testResourceArn(),
			SecretArn:   testSecretArn(),
			SQL:         "SELECT val FROM " + tableName + " ORDER BY val",
		}, &selResp)

		if len(selResp.Records) != 3 {
			t.Errorf("expected 3 rows, got %d", len(selResp.Records))
		}
	})
}

// ── HTML escaping ────────────────────────────────────────────────────────────

// TestIntegration_FormattedRecords_NoHTMLEscaping verifies the full pipeline:
// executor (json.Encoder with SetEscapeHTML(false)) → writeJSON. A SELECT that
// returns string values containing <, >, and & must produce a response body
// with those characters written literally, not as \u003c / \u003e / \u0026.
func TestIntegration_FormattedRecords_NoHTMLEscaping(t *testing.T) {
	_, srv := integrationHandler(t)

	status, raw := apiPostBytes(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn:     testResourceArn(),
		SecretArn:       testSecretArn(),
		SQL:             `SELECT '<b>hello</b> & world' AS msg`,
		FormatRecordsAs: "JSON",
	})

	if status != http.StatusOK {
		t.Fatalf("status: %d, body: %s", status, raw)
	}

	body := string(raw)
	for _, escaped := range []string{`\u003c`, `\u003e`, `\u0026`} {
		if strings.Contains(body, escaped) {
			t.Errorf("response contains HTML escape %q: %s", escaped, body)
		}
	}
	if !strings.Contains(body, "<b>hello</b>") || !strings.Contains(body, "& world") {
		t.Errorf("response missing raw HTML characters: %s", body)
	}
}

// ── Error cases ───────────────────────────────────────────────────────────────

func TestIntegration_InvalidSQL(t *testing.T) {
	_, srv := integrationHandler(t)

	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn: testResourceArn(),
		SecretArn:   testSecretArn(),
		SQL:         "THIS IS NOT SQL",
	}, nil)

	if status != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid SQL, got %d", status)
	}
}

func TestIntegration_ParameterTypes(t *testing.T) {
	_, srv := integrationHandler(t)

	var resp dataapi.ExecuteStatementResponse
	status := apiPost(t, srv, "/Execute", dataapi.ExecuteStatementRequest{
		ResourceArn: testResourceArn(),
		SecretArn:   testSecretArn(),
		SQL:         "SELECT :n::int8 AS n, :s::text AS s, :b::bool AS b",
		Parameters: []dataapi.SQLParameter{
			{Name: "n", Value: dataapi.LongField(7)},
			{Name: "s", Value: dataapi.StringField("world")},
			{Name: "b", Value: dataapi.BoolField(true)},
		},
	}, &resp)

	if status != http.StatusOK {
		t.Fatalf("status: %d", status)
	}
	if len(resp.Records) != 1 || len(resp.Records[0]) != 3 {
		t.Fatalf("unexpected record shape: %+v", resp.Records)
	}

	n := resp.Records[0][0]
	s := resp.Records[0][1]
	b := resp.Records[0][2]

	if n.LongValue == nil || *n.LongValue != 7 {
		t.Errorf("n: got %+v, want longValue=7", n)
	}
	if s.StringValue == nil || *s.StringValue != "world" {
		t.Errorf("s: got %+v, want stringValue=world", s)
	}
	if b.BooleanValue == nil || !*b.BooleanValue {
		t.Errorf("b: got %+v, want booleanValue=true", b)
	}
}
