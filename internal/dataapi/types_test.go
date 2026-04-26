package dataapi

import (
	"database/sql"
	"testing"
	"time"
)

// ── destToField ───────────────────────────────────────────────────────────────

func TestDestToField_NullInt(t *testing.T) {
	dest := &sql.NullInt64{Valid: false}
	f := destToField(dest, "INT4")
	if f.IsNull == nil || !*f.IsNull {
		t.Error("expected isNull=true for invalid NullInt64")
	}
}

func TestDestToField_ValidInt(t *testing.T) {
	dest := &sql.NullInt64{Int64: 42, Valid: true}
	f := destToField(dest, "INT4")
	if f.LongValue == nil || *f.LongValue != 42 {
		t.Errorf("expected longValue=42, got %v", f.LongValue)
	}
}

func TestDestToField_ValidFloat(t *testing.T) {
	dest := &sql.NullFloat64{Float64: 2.5, Valid: true}
	f := destToField(dest, "FLOAT8")
	if f.DoubleValue == nil || *f.DoubleValue != 2.5 {
		t.Errorf("expected doubleValue=2.5, got %v", f.DoubleValue)
	}
}

func TestDestToField_ValidBool(t *testing.T) {
	dest := &sql.NullBool{Bool: true, Valid: true}
	f := destToField(dest, "BOOL")
	if f.BooleanValue == nil || !*f.BooleanValue {
		t.Error("expected booleanValue=true")
	}
}

func TestDestToField_ValidString(t *testing.T) {
	dest := &sql.NullString{String: "hello", Valid: true}
	f := destToField(dest, "TEXT")
	if f.StringValue == nil || *f.StringValue != "hello" {
		t.Errorf("expected stringValue=hello, got %v", f.StringValue)
	}
}

func TestDestToField_NullString(t *testing.T) {
	dest := &sql.NullString{Valid: false}
	f := destToField(dest, "TEXT")
	if f.IsNull == nil || !*f.IsNull {
		t.Error("expected isNull=true for invalid NullString")
	}
}

func TestDestToField_ByteaNil(t *testing.T) {
	var b []byte
	f := destToField(&b, "BYTEA")
	if f.IsNull == nil || !*f.IsNull {
		t.Error("expected isNull=true for nil bytea")
	}
}

func TestDestToField_ByteaValue(t *testing.T) {
	b := []byte{0x01, 0x02}
	f := destToField(&b, "BYTEA")
	if f.BlobValue == nil {
		t.Error("expected blobValue for bytea")
	}
}

// ── formatTimeValue ───────────────────────────────────────────────────────────

func mustParseTime(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestFormatTimeValue_Timestamp(t *testing.T) {
	ts := mustParseTime("2006-01-02 15:04:05", "2024-03-15 10:20:30")
	got := formatTimeValue(ts, "TIMESTAMP")
	want := "2024-03-15 10:20:30"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatTimeValue_TimestampWithFractional(t *testing.T) {
	ts := mustParseTime("2006-01-02 15:04:05.000000", "2024-03-15 10:20:30.123456")
	got := formatTimeValue(ts, "TIMESTAMP")
	want := "2024-03-15 10:20:30.123456"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatTimeValue_Timestamptz(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	ts := time.Date(2024, 3, 15, 10, 20, 30, 0, loc)
	got := formatTimeValue(ts, "TIMESTAMPTZ")
	want := "2024-03-15 15:20:30+00:00"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatTimeValue_Date(t *testing.T) {
	ts := mustParseTime("2006-01-02", "2024-06-01")
	got := formatTimeValue(ts, "DATE")
	if got != "2024-06-01" {
		t.Errorf("got %q, want 2024-06-01", got)
	}
}

func TestFormatTimeValue_Time(t *testing.T) {
	ts := mustParseTime("15:04:05", "14:30:00")
	got := formatTimeValue(ts, "TIME")
	if got != "14:30:00" {
		t.Errorf("got %q, want 14:30:00", got)
	}
}

// ── stripTrailingZeros ────────────────────────────────────────────────────────

func TestStripTrailingZeros(t *testing.T) {
	tests := []struct {
		in        string
		removeDot bool
		want      string
	}{
		{"15:04:05.123000", true, "15:04:05.123"},
		{"15:04:05.000000", true, "15:04:05"},
		{"15:04:05.000000", false, "15:04:05."},
		{"15:04:05", true, "15:04:05"},
		{"15:04:05.1", true, "15:04:05.1"},
	}
	for _, tt := range tests {
		got := stripTrailingZeros(tt.in, tt.removeDot)
		if got != tt.want {
			t.Errorf("stripTrailingZeros(%q, %v) = %q, want %q", tt.in, tt.removeDot, got, tt.want)
		}
	}
}

// ── awsTypeCode ───────────────────────────────────────────────────────────────

func TestAwsTypeCode(t *testing.T) {
	tests := []struct {
		pg   string
		code int
	}{
		{"INT4", 4},
		{"INT8", -5},
		{"INT2", 5},
		{"FLOAT8", 8},
		{"BOOL", 16},
		{"TEXT", 12},
		{"BYTEA", -2},
		{"TIMESTAMP", 93},
		{"TIMESTAMPTZ", -101},
		{"DATE", 91},
		{"TIME", 92},
		{"TIMETZ", -102},
		{"UUID", 12},
		{"JSONB", 12},
		{"UNKNOWN_TYPE", 12},
	}
	for _, tt := range tests {
		got := awsTypeCode(tt.pg)
		if got != tt.code {
			t.Errorf("awsTypeCode(%q) = %d, want %d", tt.pg, got, tt.code)
		}
	}
}
