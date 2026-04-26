package dataapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// awsTypeCode maps PostgreSQL type names (as returned by
// (*sql.ColumnType).DatabaseTypeName()) to JDBC type codes. These are the
// values the AWS Data API returns in ColumnMetadata.Type.
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

// columnMetadataFromType builds a ColumnMetadata entry from a *sql.ColumnType.
func columnMetadataFromType(ct *sql.ColumnType) ColumnMetadata {
	typeName := ct.DatabaseTypeName()
	meta := ColumnMetadata{
		Name:     ct.Name(),
		Label:    ct.Name(),
		TypeName: typeName,
		Type:     awsTypeCode(typeName),
		IsSigned: isSignedType(typeName),
	}

	if precision, scale, ok := ct.DecimalSize(); ok {
		meta.Precision = int(precision)
		meta.Scale = int(scale)
	}

	if length, ok := ct.Length(); ok && meta.Precision == 0 {
		meta.Precision = int(length)
	}

	if nullable, ok := ct.Nullable(); ok {
		if nullable {
			meta.Nullable = 1 // columnNullable
		} else {
			meta.Nullable = 0 // columnNoNulls
		}
	} else {
		meta.Nullable = 2 // columnNullableUnknown
	}

	return meta
}

// makeScanDest returns a pointer of the appropriate type for scanning a column.
// Using typed scan destinations gives pgx the information it needs to return
// proper Go values rather than raw bytes.
func makeScanDest(typeName string) any {
	switch strings.ToUpper(typeName) {
	case "INT2", "INT4", "INT8", "OID", "XID":
		return new(sql.NullInt64)
	case "FLOAT4", "FLOAT8":
		return new(sql.NullFloat64)
	case "BOOL":
		return new(sql.NullBool)
	case "BYTEA":
		return new([]byte)
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "TIMETZ":
		return new(sql.NullTime)
	default:
		return new(sql.NullString)
	}
}

// destToField converts a scan destination into the appropriate AWS Data API Field.
func destToField(dest any, typeName string) Field {
	switch v := dest.(type) {
	case *sql.NullInt64:
		if !v.Valid {
			return NullField()
		}
		return LongField(v.Int64)

	case *sql.NullFloat64:
		if !v.Valid {
			return NullField()
		}
		return DoubleField(v.Float64)

	case *sql.NullBool:
		if !v.Valid {
			return NullField()
		}
		return BoolField(v.Bool)

	case *[]byte:
		if *v == nil {
			return NullField()
		}
		return BlobField(*v)

	case *sql.NullTime:
		if !v.Valid {
			return NullField()
		}
		return StringField(formatTimeValue(v.Time, typeName))

	case *sql.NullString:
		if !v.Valid {
			return NullField()
		}
		return StringField(v.String)
	}

	return NullField()
}

// formatTimeValue formats a time.Time according to the PostgreSQL type name,
// matching AWS Data API conventions: trailing fractional zeros are stripped,
// and TIMESTAMPTZ / TIMETZ values are normalised to UTC.
func formatTimeValue(t time.Time, typeName string) string {
	switch strings.ToUpper(typeName) {
	case "TIMESTAMPTZ":
		t = t.UTC()
		return stripTrailingZeros(t.Format("2006-01-02 15:04:05.999999999"), true) + "+00:00"

	case "TIMESTAMP":
		return stripTrailingZeros(t.Format("2006-01-02 15:04:05.999999999"), true)

	case "DATE":
		return t.Format("2006-01-02")

	case "TIMETZ":
		t = t.UTC()
		return stripTrailingZeros(t.Format("15:04:05.999999999"), true) + "+00:00"

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

// scanRowToFields scans the current row into a slice of Fields.
func scanRowToFields(rows *sql.Rows, colTypes []*sql.ColumnType) ([]Field, error) {
	dests := make([]any, len(colTypes))
	for i, ct := range colTypes {
		dests[i] = makeScanDest(ct.DatabaseTypeName())
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, fmt.Errorf("row scan: %w", err)
	}
	fields := make([]Field, len(colTypes))
	for i, dest := range dests {
		fields[i] = destToField(dest, colTypes[i].DatabaseTypeName())
	}
	return fields, nil
}

// scanRowToMap scans the current row into a map[string]interface{} for the
// formatRecordsAs=JSON path.
func scanRowToMap(rows *sql.Rows, colTypes []*sql.ColumnType) (map[string]any, error) {
	dests := make([]any, len(colTypes))
	for i, ct := range colTypes {
		dests[i] = makeScanDest(ct.DatabaseTypeName())
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, fmt.Errorf("row scan: %w", err)
	}
	row := make(map[string]any, len(colTypes))
	for i, ct := range colTypes {
		row[ct.Name()] = destToNative(dests[i], ct.DatabaseTypeName())
	}
	return row, nil
}

// destToNative converts a scan destination to a plain Go value for JSON output.
func destToNative(dest any, typeName string) any {
	switch v := dest.(type) {
	case *sql.NullInt64:
		if !v.Valid {
			return nil
		}
		return v.Int64
	case *sql.NullFloat64:
		if !v.Valid {
			return nil
		}
		return v.Float64
	case *sql.NullBool:
		if !v.Valid {
			return nil
		}
		return v.Bool
	case *[]byte:
		if *v == nil {
			return nil
		}
		return json.RawMessage(*v)
	case *sql.NullTime:
		if !v.Valid {
			return nil
		}
		return formatTimeValue(v.Time, typeName)
	case *sql.NullString:
		if !v.Valid {
			return nil
		}
		return v.String
	}
	return nil
}
