package dataapi

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// oidTypeName maps well-known PostgreSQL type OIDs to the short type-name
// strings used throughout this package (valueToField, awsTypeCode, etc.).
func oidTypeName(oid uint32) string {
	switch oid {
	case 16:
		return "BOOL"
	case 17:
		return "BYTEA"
	case 18:
		return "CHAR"
	case 20:
		return "INT8"
	case 21:
		return "INT2"
	case 23:
		return "INT4"
	case 25:
		return "TEXT"
	case 26:
		return "OID"
	case 28:
		return "XID"
	case 114:
		return "JSON"
	case 700:
		return "FLOAT4"
	case 701:
		return "FLOAT8"
	case 1042:
		return "BPCHAR"
	case 1043:
		return "VARCHAR"
	case 1082:
		return "DATE"
	case 1083:
		return "TIME"
	case 1114:
		return "TIMESTAMP"
	case 1184:
		return "TIMESTAMPTZ"
	case 1266:
		return "TIMETZ"
	case 1700:
		return "NUMERIC"
	case 2950:
		return "UUID"
	case 3802:
		return "JSONB"

	// Array types — name is "_" + element type name.
	// OIDs from pg_type where typtype='b' and typelem != 0.
	case 199:
		return "_JSON"
	case 1000:
		return "_BOOL"
	case 1002:
		return "_BPCHAR"
	case 1005:
		return "_INT2"
	case 1007:
		return "_INT4"
	case 1009:
		return "_TEXT"
	case 1015:
		return "_VARCHAR"
	case 1016:
		return "_INT8"
	case 1021:
		return "_FLOAT4"
	case 1022:
		return "_FLOAT8"
	case 1115:
		return "_TIMESTAMP"
	case 1182:
		return "_DATE"
	case 1185:
		return "_TIMESTAMPTZ"
	case 1231:
		return "_NUMERIC"
	case 2951:
		return "_UUID"
	case 3807:
		return "_JSONB"

	default:
		return "TEXT"
	}
}

// awsTypeCode maps PostgreSQL type names to JDBC type codes returned by the
// AWS Data API in ColumnMetadata.Type.
func awsTypeCode(typeName string) int {
	switch strings.ToUpper(typeName) {
	case "INT2":
		return 5 // SMALLINT
	case "INT4":
		return 4 // INTEGER
	case "INT8":
		return -5 // BIGINT
	case "FLOAT4":
		return 7 // REAL
	case "FLOAT8":
		return 8 // DOUBLE
	case "NUMERIC":
		return 2 // NUMERIC
	case "BOOL":
		return 16 // BOOLEAN
	case "BYTEA":
		return -2 // BINARY
	case "TIMESTAMP":
		return 93 // TIMESTAMP
	case "TIMESTAMPTZ":
		return -101 // TIMESTAMP_WITH_TIMEZONE
	case "DATE":
		return 91 // DATE
	case "TIME":
		return 92 // TIME
	case "TIMETZ":
		return -102 // TIME_WITH_TIMEZONE
	case "TEXT", "VARCHAR", "BPCHAR", "UUID", "JSON", "JSONB":
		return 12 // VARCHAR
	default:
		return 12 // VARCHAR as safe fallback
	}
}

// isSignedType returns true for PostgreSQL types that carry a sign.
func isSignedType(typeName string) bool {
	switch strings.ToUpper(typeName) {
	case "INT2", "INT4", "INT8", "FLOAT4", "FLOAT8", "NUMERIC":
		return true
	}
	return false
}

// columnMetadataFromField builds a ColumnMetadata entry from a pgconn.FieldDescription.
func columnMetadataFromField(fd pgconn.FieldDescription) ColumnMetadata {
	typeName := oidTypeName(fd.DataTypeOID)
	return ColumnMetadata{
		Name:     fd.Name,
		Label:    fd.Name,
		TypeName: typeName,
		Type:     awsTypeCode(typeName),
		IsSigned: isSignedType(typeName),
		Nullable: 2, // columnNullableUnknown — FieldDescription carries no nullability info
	}
}

// ── core conversion functions ─────────────────────────────────────────────────
//
// Both valueToField and nativeValue operate on the values returned by
// rows.Values(), which are decoded by pgx's registered codec for each OID.
//
// Timezone awareness — CRITICAL invariant:
//   - TIMESTAMP (OID 1114): pgx decodes with .UTC(), so the time.Time has a
//     UTC location.  The value must be formatted WITHOUT a timezone offset.
//   - TIMESTAMPTZ (OID 1184): pgx decodes with time.Local (no .UTC() call),
//     so the location equals time.Local.  The value must be normalised to UTC
//     and formatted WITH a +00:00 suffix.
//
// In a server environment where time.Local == time.UTC, TIMESTAMP and
// TIMESTAMPTZ produce time.Time values that are indistinguishable by
// t.Location().  The typeName parameter (derived from the column OID) is the
// ONLY reliable source of timezone awareness.  Never use t.Location() to
// decide whether to add a timezone offset.

// valueToField converts a pgx-decoded column value into an AWS Data API Field.
// typeName must be the result of oidTypeName(fd.DataTypeOID) for the column.
func valueToField(v any, typeName string) Field {
	if v == nil {
		return NullField()
	}

	// JSON/JSONB columns are decoded by pgx into Go values (map, slice, scalar).
	// Re-marshal to a JSON string so callers receive the raw text representation.
	tn := strings.ToUpper(typeName)
	if tn == "JSON" || tn == "JSONB" {
		b, err := json.Marshal(v)
		if err != nil {
			return StringField(fmt.Sprintf("%v", v))
		}
		return StringField(string(b))
	}

	switch val := v.(type) {
	case pgtype.InfinityModifier:
		// Returned by TIMESTAMP / TIMESTAMPTZ / DATE codecs for ±infinity.
		if val > 0 {
			return StringField("infinity")
		}
		return StringField("-infinity")

	case bool:
		return BoolField(val)

	case int8:
		return LongField(int64(val))
	case int16:
		return LongField(int64(val))
	case int32:
		return LongField(int64(val))
	case int64:
		return LongField(val)

	case float32:
		return DoubleField(float64(val))
	case float64:
		return DoubleField(val)

	case string:
		// Covers TEXT, VARCHAR, CHAR, UUID, NUMERIC (text fallback), and TIMETZ
		// (OID 1266, not registered in pgx's default type map; arrives as raw
		// text like "14:30:00+05:30", preserving the original offset).
		return StringField(val)

	case time.Time:
		// typeName drives format: DATE → date only, TIMESTAMP → no offset,
		// TIMESTAMPTZ → UTC-normalised with +00:00.  See timezone note above.
		return StringField(formatTimeValue(val, typeName))

	case pgtype.Time:
		// TIME (OID 1083): microseconds since midnight, no date, no timezone.
		// TIMETZ (OID 1266) is not registered in pgx and arrives as string above.
		if !val.Valid {
			return NullField()
		}
		t := time.Time{}.Add(time.Duration(val.Microseconds) * time.Microsecond)
		return StringField(formatTimeValue(t, typeName))

	case []byte:
		// BYTEA: base64-encoded when JSON-serialised.
		return BlobField(val)

	case [16]byte:
		// UUID is decoded as a raw [16]byte by pgx.
		return StringField(formatUUID(val))

	case pgtype.Numeric:
		return StringField(numericToString(val))

	case []any:
		// PostgreSQL array, decoded by ArrayCodec.DecodeValue into []any where
		// each element is the natural Go type for the element OID.
		return Field{ArrayValue: arrayToField(val, typeName)}
	}

	// Fallback for pgtype extension types, custom domains, etc.
	return StringField(fmt.Sprintf("%v", v))
}

// nativeValue converts a pgx-decoded column value to a JSON-serialisable Go
// value for the formattedRecords output path.
// typeName must be the result of oidTypeName(fd.DataTypeOID) for the column.
func nativeValue(v any, typeName string) any {
	if v == nil {
		return nil
	}

	// JSON/JSONB: pgx has already unmarshalled the value; return it as-is so
	// json.Marshal embeds it directly rather than double-encoding.
	tn := strings.ToUpper(typeName)
	if tn == "JSON" || tn == "JSONB" {
		return v
	}

	switch val := v.(type) {
	case pgtype.InfinityModifier:
		if val > 0 {
			return "infinity"
		}
		return "-infinity"

	case bool:
		return val

	case int8:
		return int64(val)
	case int16:
		return int64(val)
	case int32:
		return int64(val)
	case int64:
		return val

	case float32:
		return float64(val)
	case float64:
		return val

	case string:
		return val

	case time.Time:
		// typeName controls TZ formatting — see timezone note above.
		return formatTimeValue(val, typeName)

	case pgtype.Time:
		if !val.Valid {
			return nil
		}
		t := time.Time{}.Add(time.Duration(val.Microseconds) * time.Microsecond)
		return formatTimeValue(t, typeName)

	case []byte:
		// BYTEA: return as []byte; json.Marshal will produce a base64 string.
		return val

	case [16]byte:
		return formatUUID(val)

	case pgtype.Numeric:
		return numericToString(val)

	case []any:
		// Recursively convert each element, passing the element type name so
		// that TIMESTAMP vs TIMESTAMPTZ distinctions are preserved inside arrays.
		elemType := strings.TrimPrefix(typeName, "_")
		result := make([]any, len(val))
		for i, elem := range val {
			result[i] = nativeValue(elem, elemType)
		}
		return result
	}

	return fmt.Sprintf("%v", v)
}

// arrayToField converts a []any of pgx-decoded array elements (as returned by
// ArrayCodec.DecodeValue) into an *ArrayValue. typeName is the array OID name
// (e.g. "_TIMESTAMP", "_TIMESTAMPTZ") and is used to format time elements
// correctly — this is what preserves TIMESTAMP vs TIMESTAMPTZ distinction.
func arrayToField(elements []any, typeName string) *ArrayValue {
	av := &ArrayValue{}
	elemType := strings.TrimPrefix(typeName, "_")

	for _, elem := range elements {
		if elem == nil {
			// NULL array elements have no per-element null concept in ArrayValue;
			// skip them rather than inserting a misleading zero value.
			continue
		}
		switch val := elem.(type) {
		case pgtype.InfinityModifier:
			if val > 0 {
				av.StringValues = append(av.StringValues, "infinity")
			} else {
				av.StringValues = append(av.StringValues, "-infinity")
			}
		case bool:
			av.BooleanValues = append(av.BooleanValues, val)
		case int8:
			av.LongValues = append(av.LongValues, int64(val))
		case int16:
			av.LongValues = append(av.LongValues, int64(val))
		case int32:
			av.LongValues = append(av.LongValues, int64(val))
		case int64:
			av.LongValues = append(av.LongValues, val)
		case float32:
			av.DoubleValues = append(av.DoubleValues, float64(val))
		case float64:
			av.DoubleValues = append(av.DoubleValues, val)
		case string:
			av.StringValues = append(av.StringValues, val)
		case time.Time:
			// elemType is e.g. "TIMESTAMP" or "TIMESTAMPTZ" — drives TZ formatting.
			av.StringValues = append(av.StringValues, formatTimeValue(val, elemType))
		case pgtype.Time:
			if val.Valid {
				t := time.Time{}.Add(time.Duration(val.Microseconds) * time.Microsecond)
				av.StringValues = append(av.StringValues, formatTimeValue(t, elemType))
			}
		case pgtype.Numeric:
			av.StringValues = append(av.StringValues, numericToString(val))
		case [16]byte:
			av.StringValues = append(av.StringValues, formatUUID(val))
		default:
			// JSON objects/arrays and unknown extension types: marshal to string.
			b, _ := json.Marshal(val)
			av.StringValues = append(av.StringValues, string(b))
		}
	}
	return av
}

// formatUUID formats a raw [16]byte UUID as a canonical lowercase hyphenated
// string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// numericToString converts a pgtype.Numeric to its exact decimal string
// representation (e.g. "123.456"), preserving full precision.
func numericToString(n pgtype.Numeric) string {
	if !n.Valid {
		return "0"
	}
	if n.NaN {
		return "NaN"
	}
	if n.InfinityModifier != pgtype.Finite {
		if n.InfinityModifier > 0 {
			return "Infinity"
		}
		return "-Infinity"
	}
	if n.Int == nil {
		return "0"
	}

	s := n.Int.String() // decimal digits, possibly with leading "-"
	exp := int(n.Exp)

	switch {
	case exp == 0:
		return s
	case exp > 0:
		// Shift decimal point right: append zeros.
		return s + strings.Repeat("0", exp)
	default:
		// exp < 0: insert decimal point.
		neg := len(s) > 0 && s[0] == '-'
		digits := s
		if neg {
			digits = s[1:]
		}
		dotPos := len(digits) + exp // position of decimal point from the left
		var r string
		switch {
		case dotPos <= 0:
			// All digits are to the right of the decimal point.
			r = "0." + strings.Repeat("0", -dotPos) + digits
		case dotPos < len(digits):
			r = digits[:dotPos] + "." + digits[dotPos:]
		default:
			// dotPos >= len(digits): no fractional part needed.
			r = digits + strings.Repeat("0", dotPos-len(digits))
		}
		if neg {
			return "-" + r
		}
		return r
	}
}

// formatTimeValue formats a time.Time according to the PostgreSQL type name,
// matching AWS Data API conventions.
//
// Timezone rules:
//   - TIMESTAMP:   wall-clock value, no offset  → "2006-01-02 15:04:05[.ffffff]"
//   - TIMESTAMPTZ: normalised to UTC, +00:00     → "2006-01-02 15:04:05[.ffffff]+00:00"
//   - DATE:        date only                     → "2006-01-02"
//   - TIMETZ:      normalised to UTC, +00:00     → "15:04:05[.ffffff]+00:00"
//   - TIME:        time only, no offset          → "15:04:05[.ffffff]"
//
// Trailing fractional zeros are stripped in all cases.
func formatTimeValue(t time.Time, typeName string) string {
	switch strings.ToUpper(typeName) {
	case "TIMESTAMPTZ":
		// AWS Data API always returns TIMESTAMPTZ normalised to UTC with no
		// offset suffix. The value is UTC but the format is identical to
		// TIMESTAMP so Python (and other clients) receive a naive datetime
		// string and can do arithmetic across both column types without a
		// TypeError from mixing aware and naive datetimes.
		t = t.UTC()
		return stripTrailingZeros(t.Format("2006-01-02 15:04:05.999999999"), true)

	case "TIMESTAMP":
		return stripTrailingZeros(t.Format("2006-01-02 15:04:05.999999999"), true)

	case "DATE":
		return t.Format("2006-01-02")

	case "TIMETZ":
		// Same reasoning as TIMESTAMPTZ: normalise to UTC, no offset suffix.
		t = t.UTC()
		return stripTrailingZeros(t.Format("15:04:05.999999999"), true)

	case "TIME":
		return stripTrailingZeros(t.Format("15:04:05.999999999"), true)

	default:
		return t.Format(time.RFC3339Nano)
	}
}

// stripTrailingZeros removes trailing zeros from the fractional seconds part
// of a formatted time string. If removeDot is true it also removes a trailing
// decimal point (e.g. "15:04:05." → "15:04:05").
func stripTrailingZeros(s string, removeDot bool) string {
	dot := strings.LastIndex(s, ".")
	if dot == -1 {
		return s
	}
	end := len(s)
	for end > dot+1 && s[end-1] == '0' {
		end--
	}
	if removeDot && end == dot+1 {
		end = dot
	}
	return s[:end]
}

// ── row scanning ──────────────────────────────────────────────────────────────

// scanRowToFields decodes the current row into a slice of AWS Data API Fields.
// It uses rows.Values() so that pgx's registered codec handles all type
// decoding; the column OID is used only to drive time and array formatting.
func scanRowToFields(rows pgx.Rows, descs []pgconn.FieldDescription) ([]Field, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("row values: %w", err)
	}
	fields := make([]Field, len(values))
	for i, v := range values {
		fields[i] = valueToField(v, oidTypeName(descs[i].DataTypeOID))
	}
	return fields, nil
}

// scanRowToMap decodes the current row into a map[string]any for the
// formatRecordsAs=JSON path.
func scanRowToMap(rows pgx.Rows, descs []pgconn.FieldDescription) (map[string]any, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, fmt.Errorf("row values: %w", err)
	}
	row := make(map[string]any, len(values))
	for i, fd := range descs {
		row[fd.Name] = nativeValue(values[i], oidTypeName(fd.DataTypeOID))
	}
	return row, nil
}
