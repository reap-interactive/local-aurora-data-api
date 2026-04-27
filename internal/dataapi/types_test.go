package dataapi

import (
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func mustParseTime(layout, s string) time.Time {
	t, err := time.Parse(layout, s)
	if err != nil {
		panic(err)
	}
	return t
}

// ── valueToField — scalars ────────────────────────────────────────────────────

func TestValueToField_Null(t *testing.T) {
	f := valueToField(nil, "TEXT")
	if f.IsNull == nil || !*f.IsNull {
		t.Error("expected isNull=true for nil")
	}
}

func TestValueToField_Bool(t *testing.T) {
	f := valueToField(true, "BOOL")
	if f.BooleanValue == nil || !*f.BooleanValue {
		t.Error("expected booleanValue=true")
	}
}

func TestValueToField_Int32(t *testing.T) {
	f := valueToField(int32(42), "INT4")
	if f.LongValue == nil || *f.LongValue != 42 {
		t.Errorf("expected longValue=42, got %v", f.LongValue)
	}
}

func TestValueToField_Int64(t *testing.T) {
	f := valueToField(int64(99), "INT8")
	if f.LongValue == nil || *f.LongValue != 99 {
		t.Errorf("expected longValue=99, got %v", f.LongValue)
	}
}

func TestValueToField_Int16(t *testing.T) {
	f := valueToField(int16(7), "INT2")
	if f.LongValue == nil || *f.LongValue != 7 {
		t.Errorf("expected longValue=7, got %v", f.LongValue)
	}
}

func TestValueToField_Float64(t *testing.T) {
	f := valueToField(float64(2.5), "FLOAT8")
	if f.DoubleValue == nil || *f.DoubleValue != 2.5 {
		t.Errorf("expected doubleValue=2.5, got %v", f.DoubleValue)
	}
}

func TestValueToField_Float32(t *testing.T) {
	f := valueToField(float32(1.5), "FLOAT4")
	if f.DoubleValue == nil || *f.DoubleValue != float64(float32(1.5)) {
		t.Errorf("expected doubleValue, got %v", f.DoubleValue)
	}
}

func TestValueToField_String(t *testing.T) {
	f := valueToField("hello", "TEXT")
	if f.StringValue == nil || *f.StringValue != "hello" {
		t.Errorf("expected stringValue=hello, got %v", f.StringValue)
	}
}

func TestValueToField_Bytea(t *testing.T) {
	f := valueToField([]byte{0x01, 0x02}, "BYTEA")
	if f.BlobValue == nil {
		t.Error("expected blobValue for []byte")
	}
}

func TestValueToField_UUID(t *testing.T) {
	b := [16]byte{
		0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}
	f := valueToField(b, "UUID")
	if f.StringValue == nil {
		t.Fatal("expected stringValue for UUID")
	}
	want := "550e8400-e29b-41d4-a716-446655440000"
	if *f.StringValue != want {
		t.Errorf("UUID: got %q, want %q", *f.StringValue, want)
	}
}

// ── valueToField — infinity ───────────────────────────────────────────────────

func TestValueToField_Infinity(t *testing.T) {
	f := valueToField(pgtype.Infinity, "TIMESTAMP")
	if f.StringValue == nil || *f.StringValue != "infinity" {
		t.Errorf("expected stringValue=infinity, got %+v", f)
	}
}

func TestValueToField_NegativeInfinity(t *testing.T) {
	f := valueToField(pgtype.NegativeInfinity, "DATE")
	if f.StringValue == nil || *f.StringValue != "-infinity" {
		t.Errorf("expected stringValue=-infinity, got %+v", f)
	}
}

// ── valueToField — timezone disambiguation ────────────────────────────────────
//
// TIMESTAMP and TIMESTAMPTZ both arrive as time.Time from rows.Values().
// In a UTC server environment their Location() values are identical.
// typeName is the ONLY authority on whether a timezone offset is emitted.

func TestValueToField_Timestamp_NoOffset(t *testing.T) {
	// Wall-clock time in UTC — TIMESTAMP must NOT carry a timezone offset.
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	f := valueToField(ts, "TIMESTAMP")
	if f.StringValue == nil {
		t.Fatal("expected stringValue")
	}
	want := "2024-01-15 10:00:00"
	if *f.StringValue != want {
		t.Errorf("TIMESTAMP: got %q, want %q", *f.StringValue, want)
	}
	if contains(*f.StringValue, "+") {
		t.Errorf("TIMESTAMP: must not contain '+', got %q", *f.StringValue)
	}
}

func TestValueToField_Timestamptz_UTCOffset(t *testing.T) {
	// Same wall clock, TIMESTAMPTZ normalises to UTC — no +00:00 suffix
	// (AWS Data API always returns naive UTC strings).
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	f := valueToField(ts, "TIMESTAMPTZ")
	if f.StringValue == nil {
		t.Fatal("expected stringValue")
	}
	want := "2024-01-15 10:00:00"
	if *f.StringValue != want {
		t.Errorf("TIMESTAMPTZ: got %q, want %q", *f.StringValue, want)
	}
}

func TestValueToField_Timestamptz_NormalisedToUTC(t *testing.T) {
	// time.Time in UTC+5: 10:00 UTC+5 == 05:00 UTC.
	// TIMESTAMPTZ normalises to UTC but emits no offset suffix.
	loc := time.FixedZone("UTC+5", 5*60*60)
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, loc)
	f := valueToField(ts, "TIMESTAMPTZ")
	if f.StringValue == nil {
		t.Fatal("expected stringValue")
	}
	want := "2024-01-15 05:00:00"
	if *f.StringValue != want {
		t.Errorf("TIMESTAMPTZ: got %q, want %q", *f.StringValue, want)
	}
}

func TestValueToField_Date_NoTimeComponent(t *testing.T) {
	ts := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	f := valueToField(ts, "DATE")
	if f.StringValue == nil || *f.StringValue != "2024-06-01" {
		t.Errorf("DATE: got %v", f.StringValue)
	}
}

func TestValueToField_SameTime_TimestampVsTimestamptz_UTCNormalisation(t *testing.T) {
	// Key invariant: TIMESTAMPTZ normalises its value to UTC; TIMESTAMP keeps
	// the wall-clock reading.  With a non-UTC input the two produce DIFFERENT
	// UTC strings even though the format is identical (no offset suffix on
	// either — AWS Data API always returns naive UTC datetimes).
	loc := time.FixedZone("UTC+5", 5*60*60)
	ts := time.Date(2024, 3, 15, 10, 20, 30, 0, loc) // 10:20:30 UTC+5 = 05:20:30 UTC

	noTZ := valueToField(ts, "TIMESTAMP")     // keeps wall clock: 10:20:30
	withTZ := valueToField(ts, "TIMESTAMPTZ") // normalises to UTC: 05:20:30

	if noTZ.StringValue == nil || withTZ.StringValue == nil {
		t.Fatal("expected stringValue from both")
	}
	if *noTZ.StringValue == *withTZ.StringValue {
		t.Errorf("non-UTC input must produce different UTC strings; both = %q", *noTZ.StringValue)
	}
	if *noTZ.StringValue != "2024-03-15 10:20:30" {
		t.Errorf("TIMESTAMP (wall clock): got %q, want \"2024-03-15 10:20:30\"", *noTZ.StringValue)
	}
	if *withTZ.StringValue != "2024-03-15 05:20:30" {
		t.Errorf("TIMESTAMPTZ (UTC): got %q, want \"2024-03-15 05:20:30\"", *withTZ.StringValue)
	}
	// Neither result must carry an offset suffix.
	for _, s := range []*string{noTZ.StringValue, withTZ.StringValue} {
		if contains(*s, "+") {
			t.Errorf("AWS Data API must not include offset suffix, got %q", *s)
		}
	}
}

// ── valueToField — numeric ────────────────────────────────────────────────────

func TestValueToField_Numeric(t *testing.T) {
	tests := []struct {
		n    pgtype.Numeric
		want string
	}{
		{makeNumeric(t, "12345", 0), "12345"},
		{makeNumeric(t, "12345", -2), "123.45"},
		{makeNumeric(t, "12345", 2), "1234500"},
		{makeNumeric(t, "-12345", -2), "-123.45"},
		{makeNumeric(t, "1", -4), "0.0001"},
	}
	for _, tt := range tests {
		f := valueToField(tt.n, "NUMERIC")
		if f.StringValue == nil || *f.StringValue != tt.want {
			t.Errorf("numeric(%v exp=%d): got %v, want %q", tt.n.Int, tt.n.Exp, f.StringValue, tt.want)
		}
	}
}

// ── valueToField — arrays ─────────────────────────────────────────────────────

func TestValueToField_TextArray(t *testing.T) {
	f := valueToField([]any{"alpha", "beta", "gamma"}, "_TEXT")
	if f.ArrayValue == nil {
		t.Fatal("expected arrayValue")
	}
	if len(f.ArrayValue.StringValues) != 3 || f.ArrayValue.StringValues[1] != "beta" {
		t.Errorf("unexpected stringValues: %v", f.ArrayValue.StringValues)
	}
}

func TestValueToField_Int4Array(t *testing.T) {
	f := valueToField([]any{int32(1), int32(2), int32(3)}, "_INT4")
	if f.ArrayValue == nil || len(f.ArrayValue.LongValues) != 3 {
		t.Fatalf("unexpected field: %+v", f)
	}
	if f.ArrayValue.LongValues[2] != 3 {
		t.Errorf("unexpected longValues: %v", f.ArrayValue.LongValues)
	}
}

func TestValueToField_Int8Array(t *testing.T) {
	f := valueToField([]any{int64(10), int64(20)}, "_INT8")
	if f.ArrayValue == nil || len(f.ArrayValue.LongValues) != 2 {
		t.Errorf("unexpected field: %+v", f)
	}
}

func TestValueToField_Float8Array(t *testing.T) {
	f := valueToField([]any{float64(1.1), float64(2.2)}, "_FLOAT8")
	if f.ArrayValue == nil || len(f.ArrayValue.DoubleValues) != 2 {
		t.Errorf("unexpected field: %+v", f)
	}
}

func TestValueToField_BoolArray(t *testing.T) {
	f := valueToField([]any{true, false, true}, "_BOOL")
	if f.ArrayValue == nil || len(f.ArrayValue.BooleanValues) != 3 {
		t.Errorf("unexpected field: %+v", f)
	}
}

func TestValueToField_NullArray(t *testing.T) {
	// nil value (SQL NULL array) must produce isNull=true.
	f := valueToField(nil, "_TEXT")
	if f.IsNull == nil || !*f.IsNull {
		t.Error("expected isNull=true for nil array")
	}
}

func TestValueToField_EmptyArray(t *testing.T) {
	f := valueToField([]any{}, "_TEXT")
	if f.ArrayValue == nil {
		t.Fatal("expected arrayValue for empty array")
	}
	if len(f.ArrayValue.StringValues) != 0 {
		t.Errorf("expected empty stringValues, got %v", f.ArrayValue.StringValues)
	}
}

// ── valueToField — array timezone disambiguation ──────────────────────────────

func TestValueToField_TimestampArray_NoOffset(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	f := valueToField([]any{ts}, "_TIMESTAMP")
	if f.ArrayValue == nil || len(f.ArrayValue.StringValues) != 1 {
		t.Fatalf("unexpected field: %+v", f)
	}
	want := "2024-01-15 10:00:00"
	if f.ArrayValue.StringValues[0] != want {
		t.Errorf("_TIMESTAMP array element: got %q, want %q", f.ArrayValue.StringValues[0], want)
	}
}

func TestValueToField_TimestamptzArray_WithOffset(t *testing.T) {
	// UTC+5: 10:00 local == 05:00 UTC.  No offset suffix in output.
	loc := time.FixedZone("UTC+5", 5*60*60)
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, loc)
	f := valueToField([]any{ts}, "_TIMESTAMPTZ")
	if f.ArrayValue == nil || len(f.ArrayValue.StringValues) != 1 {
		t.Fatalf("unexpected field: %+v", f)
	}
	want := "2024-01-15 05:00:00"
	if f.ArrayValue.StringValues[0] != want {
		t.Errorf("_TIMESTAMPTZ array element: got %q, want %q", f.ArrayValue.StringValues[0], want)
	}
}

// ── nativeValue — scalars ─────────────────────────────────────────────────────

func TestNativeValue_Null(t *testing.T) {
	if got := nativeValue(nil, "TEXT"); got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestNativeValue_Bool(t *testing.T) {
	if got := nativeValue(true, "BOOL"); got != true {
		t.Errorf("expected true, got %v", got)
	}
}

func TestNativeValue_Int32WidenedToInt64(t *testing.T) {
	got := nativeValue(int32(42), "INT4")
	if v, ok := got.(int64); !ok || v != 42 {
		t.Errorf("expected int64(42), got %T(%v)", got, got)
	}
}

func TestNativeValue_Float32WidenedToFloat64(t *testing.T) {
	got := nativeValue(float32(1.5), "FLOAT4")
	if _, ok := got.(float64); !ok {
		t.Errorf("expected float64, got %T", got)
	}
}

func TestNativeValue_String(t *testing.T) {
	if got := nativeValue("hello", "TEXT"); got != "hello" {
		t.Errorf("expected hello, got %v", got)
	}
}

func TestNativeValue_Infinity(t *testing.T) {
	if got := nativeValue(pgtype.Infinity, "TIMESTAMP"); got != "infinity" {
		t.Errorf("expected infinity, got %v", got)
	}
	if got := nativeValue(pgtype.NegativeInfinity, "DATE"); got != "-infinity" {
		t.Errorf("expected -infinity, got %v", got)
	}
}

// ── nativeValue — timezone disambiguation ─────────────────────────────────────

func TestNativeValue_Timestamp_NoOffset(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	got := nativeValue(ts, "TIMESTAMP")
	want := "2024-01-15 10:00:00"
	if got != want {
		t.Errorf("TIMESTAMP: got %q, want %q", got, want)
	}
}

func TestNativeValue_Timestamptz_UTCOffset(t *testing.T) {
	// AWS returns bare UTC string — no +00:00 suffix.
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	got := nativeValue(ts, "TIMESTAMPTZ")
	want := "2024-01-15 10:00:00"
	if got != want {
		t.Errorf("TIMESTAMPTZ: got %q, want %q", got, want)
	}
}

func TestNativeValue_SameTime_TimestampVsTimestamptz_UTCNormalisation(t *testing.T) {
	// Non-UTC input so TIMESTAMPTZ (UTC-normalised) != TIMESTAMP (wall clock).
	loc := time.FixedZone("UTC+5", 5*60*60)
	ts := time.Date(2024, 3, 15, 10, 20, 30, 0, loc)
	noTZ := nativeValue(ts, "TIMESTAMP")     // 10:20:30 wall clock
	withTZ := nativeValue(ts, "TIMESTAMPTZ") // 05:20:30 UTC
	if noTZ == withTZ {
		t.Errorf("non-UTC input: TIMESTAMP and TIMESTAMPTZ must produce different values, both = %q", noTZ)
	}
	if noTZ != "2024-03-15 10:20:30" {
		t.Errorf("TIMESTAMP: got %q", noTZ)
	}
	if withTZ != "2024-03-15 05:20:30" {
		t.Errorf("TIMESTAMPTZ: got %q", withTZ)
	}
}

// ── nativeValue — arrays ──────────────────────────────────────────────────────

func TestNativeValue_TextArray(t *testing.T) {
	got := nativeValue([]any{"x", "y"}, "_TEXT")
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 2 || arr[0] != "x" || arr[1] != "y" {
		t.Errorf("unexpected value: %v", arr)
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `["x","y"]` {
		t.Errorf("JSON: got %s, want [\"x\",\"y\"]", string(b))
	}
}

func TestNativeValue_Int4Array(t *testing.T) {
	got := nativeValue([]any{int32(7), int32(8), int32(9)}, "_INT4")
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	for i, want := range []int64{7, 8, 9} {
		if arr[i] != want {
			t.Errorf("arr[%d]: got %v (%T), want %v", i, arr[i], arr[i], want)
		}
	}
	b, _ := json.Marshal(got)
	if string(b) != `[7,8,9]` {
		t.Errorf("JSON: got %s, want [7,8,9]", string(b))
	}
}

func TestNativeValue_NullArray(t *testing.T) {
	// nil top-level value = SQL NULL array.
	if got := nativeValue(nil, "_TEXT"); got != nil {
		t.Errorf("expected nil for null array, got %v", got)
	}
}

func TestNativeValue_EmptyArray(t *testing.T) {
	got := nativeValue([]any{}, "_TEXT")
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 0 {
		t.Errorf("expected empty slice, got %v", arr)
	}
	b, _ := json.Marshal(got)
	if string(b) != `[]` {
		t.Errorf("JSON: got %s, want []", string(b))
	}
}

func TestNativeValue_TimestamptzArray_ElementsNormalisedToUTC(t *testing.T) {
	// UTC+5: 10:00 local == 05:00 UTC.  No offset suffix.
	loc := time.FixedZone("UTC+5", 5*60*60)
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, loc)
	got := nativeValue([]any{ts}, "_TIMESTAMPTZ")
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 1 || arr[0] != "2024-01-15 05:00:00" {
		t.Errorf("_TIMESTAMPTZ array: got %v", arr)
	}
}

func TestNativeValue_TimestampArray_ElementsHaveNoOffset(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	got := nativeValue([]any{ts}, "_TIMESTAMP")
	arr, ok := got.([]any)
	if !ok {
		t.Fatalf("expected []any, got %T", got)
	}
	if len(arr) != 1 || arr[0] != "2024-01-15 10:00:00" {
		t.Errorf("_TIMESTAMP array: got %v", arr)
	}
}

// ── formatTimeValue ───────────────────────────────────────────────────────────

func TestFormatTimeValue_Timestamp(t *testing.T) {
	ts := mustParseTime("2006-01-02 15:04:05", "2024-03-15 10:20:30")
	if got := formatTimeValue(ts, "TIMESTAMP"); got != "2024-03-15 10:20:30" {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimeValue_TimestampWithFractional(t *testing.T) {
	ts := mustParseTime("2006-01-02 15:04:05.000000", "2024-03-15 10:20:30.123456")
	if got := formatTimeValue(ts, "TIMESTAMP"); got != "2024-03-15 10:20:30.123456" {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimeValue_Timestamptz(t *testing.T) {
	loc := time.FixedZone("UTC-5", -5*60*60)
	ts := time.Date(2024, 3, 15, 10, 20, 30, 0, loc)
	// AWS always returns TIMESTAMPTZ as bare UTC — no +00:00 suffix.
	if got := formatTimeValue(ts, "TIMESTAMPTZ"); got != "2024-03-15 15:20:30" {
		t.Errorf("got %q, want \"2024-03-15 15:20:30\"", got)
	}
}

func TestFormatTimeValue_Date(t *testing.T) {
	ts := mustParseTime("2006-01-02", "2024-06-01")
	if got := formatTimeValue(ts, "DATE"); got != "2024-06-01" {
		t.Errorf("got %q", got)
	}
}

func TestFormatTimeValue_Time(t *testing.T) {
	ts := mustParseTime("15:04:05", "14:30:00")
	if got := formatTimeValue(ts, "TIME"); got != "14:30:00" {
		t.Errorf("got %q", got)
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
		if got := stripTrailingZeros(tt.in, tt.removeDot); got != tt.want {
			t.Errorf("stripTrailingZeros(%q, %v) = %q, want %q", tt.in, tt.removeDot, got, tt.want)
		}
	}
}

// ── numericToString ───────────────────────────────────────────────────────────

func TestNumericToString(t *testing.T) {
	tests := []struct {
		n    pgtype.Numeric
		want string
	}{
		{makeNumeric(t, "0", 0), "0"},
		{makeNumeric(t, "12345", 0), "12345"},
		{makeNumeric(t, "12345", -2), "123.45"},
		{makeNumeric(t, "12345", 2), "1234500"},
		{makeNumeric(t, "-12345", -2), "-123.45"},
		{makeNumeric(t, "1", -4), "0.0001"},
		{makeNumeric(t, "12345", -7), "0.0012345"},
	}
	for _, tt := range tests {
		if got := numericToString(tt.n); got != tt.want {
			t.Errorf("numericToString(%v, exp=%d): got %q, want %q", tt.n.Int, tt.n.Exp, got, tt.want)
		}
	}
}

func TestNumericToString_NaN(t *testing.T) {
	n := pgtype.Numeric{NaN: true, Valid: true}
	if got := numericToString(n); got != "NaN" {
		t.Errorf("got %q", got)
	}
}

// ── formatUUID ────────────────────────────────────────────────────────────────

func TestFormatUUID(t *testing.T) {
	b := [16]byte{
		0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
		0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
	}
	want := "550e8400-e29b-41d4-a716-446655440000"
	if got := formatUUID(b); got != want {
		t.Errorf("formatUUID: got %q, want %q", got, want)
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
		if got := awsTypeCode(tt.pg); got != tt.code {
			t.Errorf("awsTypeCode(%q) = %d, want %d", tt.pg, got, tt.code)
		}
	}
}

// ── oidTypeName — array OIDs ──────────────────────────────────────────────────

func TestOidTypeName_ArrayOIDs(t *testing.T) {
	tests := []struct {
		oid  uint32
		want string
	}{
		{1009, "_TEXT"},
		{1015, "_VARCHAR"},
		{1007, "_INT4"},
		{1016, "_INT8"},
		{1005, "_INT2"},
		{1021, "_FLOAT4"},
		{1022, "_FLOAT8"},
		{1000, "_BOOL"},
		{1182, "_DATE"},
		{1115, "_TIMESTAMP"},
		{1185, "_TIMESTAMPTZ"},
		{1231, "_NUMERIC"},
		{2951, "_UUID"},
		{3807, "_JSONB"},
	}
	for _, tt := range tests {
		if got := oidTypeName(tt.oid); got != tt.want {
			t.Errorf("oidTypeName(%d) = %q, want %q", tt.oid, got, tt.want)
		}
	}
}

// ── test helpers ──────────────────────────────────────────────────────────────

// makeNumeric builds a pgtype.Numeric from a decimal string and an exponent
// adjustment. intStr is the unscaled integer (e.g. "12345"), exp is the base-10
// exponent (e.g. -2 means the value is 12345 × 10⁻² = 123.45).
func makeNumeric(t *testing.T, intStr string, exp int32) pgtype.Numeric {
	t.Helper()
	var i big.Int
	if _, ok := i.SetString(intStr, 10); !ok {
		t.Fatalf("makeNumeric: invalid integer string %q", intStr)
	}
	return pgtype.Numeric{Int: &i, Exp: exp, Valid: true}
}

func contains(s, substr string) bool { return len(s) >= len(substr) && indexString(s, substr) >= 0 }
func indexString(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
