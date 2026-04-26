package dataapi

import (
	"testing"
)

func TestParseNamedParams_NoParams(t *testing.T) {
	sql, args, err := ParseNamedParams("SELECT 1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT 1" {
		t.Errorf("got %q, want %q", sql, "SELECT 1")
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestParseNamedParams_SingleParam(t *testing.T) {
	params := map[string]any{"id": int64(42)}
	sql, args, err := ParseNamedParams("SELECT * FROM t WHERE id = :id", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "SELECT * FROM t WHERE id = $1"
	if sql != want {
		t.Errorf("got %q, want %q", sql, want)
	}
	if len(args) != 1 || args[0] != int64(42) {
		t.Errorf("got args %v, want [42]", args)
	}
}

func TestParseNamedParams_MultipleParams(t *testing.T) {
	params := map[string]any{"name": "alice", "age": int64(30)}
	sql, args, err := ParseNamedParams(
		"INSERT INTO users (name, age) VALUES (:name, :age)",
		params,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL := "INSERT INTO users (name, age) VALUES ($1, $2)"
	if sql != wantSQL {
		t.Errorf("got %q, want %q", sql, wantSQL)
	}
	if len(args) != 2 {
		t.Fatalf("want 2 args, got %d", len(args))
	}
	if args[0] != "alice" {
		t.Errorf("args[0]: got %v, want alice", args[0])
	}
	if args[1] != int64(30) {
		t.Errorf("args[1]: got %v, want 30", args[1])
	}
}

func TestParseNamedParams_ReusedParam(t *testing.T) {
	params := map[string]any{"val": "x"}
	sql, args, err := ParseNamedParams("SELECT :val AS a, :val AS b", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "SELECT $1 AS a, $1 AS b"
	if sql != want {
		t.Errorf("got %q, want %q", sql, want)
	}
	if len(args) != 1 {
		t.Errorf("want 1 arg (reused), got %d", len(args))
	}
}

func TestParseNamedParams_StringLiteralIgnored(t *testing.T) {
	params := map[string]any{}
	sql, args, err := ParseNamedParams("SELECT ':notaparam'", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT ':notaparam'" {
		t.Errorf("got %q, want unchanged", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestParseNamedParams_LineCommentIgnored(t *testing.T) {
	params := map[string]any{}
	sql, args, err := ParseNamedParams("SELECT 1 -- :notaparam", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT 1 -- :notaparam" {
		t.Errorf("got %q, want unchanged", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestParseNamedParams_BlockCommentIgnored(t *testing.T) {
	params := map[string]any{}
	sql, args, err := ParseNamedParams("SELECT /* :notaparam */ 1", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT /* :notaparam */ 1" {
		t.Errorf("got %q, want unchanged", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestParseNamedParams_DollarQuoteIgnored(t *testing.T) {
	params := map[string]any{}
	sql, args, err := ParseNamedParams("SELECT $$:notaparam$$", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT $$:notaparam$$" {
		t.Errorf("got %q, want unchanged", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args, got %v", args)
	}
}

func TestParseNamedParams_MissingParam(t *testing.T) {
	params := map[string]any{}
	_, _, err := ParseNamedParams("SELECT :missing", params)
	if err == nil {
		t.Fatal("expected error for missing parameter, got nil")
	}
}

func TestParseNamedParams_ParamAndLiteral(t *testing.T) {
	params := map[string]any{"id": int64(7)}
	sql, args, err := ParseNamedParams("SELECT :id, ':notparam'", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "SELECT $1, ':notparam'"
	if sql != want {
		t.Errorf("got %q, want %q", sql, want)
	}
	if len(args) != 1 || args[0] != int64(7) {
		t.Errorf("got args %v, want [7]", args)
	}
}

func TestParseNamedParams_UnderscoreAndDigitsInName(t *testing.T) {
	params := map[string]any{"user_id_1": int64(99)}
	sql, args, err := ParseNamedParams("SELECT :user_id_1", params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "SELECT $1" {
		t.Errorf("got %q, want \"SELECT $1\"", sql)
	}
	if len(args) != 1 || args[0] != int64(99) {
		t.Errorf("got args %v", args)
	}
}

func TestParseNamedParams_EmptySQL(t *testing.T) {
	sql, args, err := ParseNamedParams("", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "" {
		t.Errorf("got %q, want empty string", sql)
	}
	if len(args) != 0 {
		t.Errorf("expected no args")
	}
}
