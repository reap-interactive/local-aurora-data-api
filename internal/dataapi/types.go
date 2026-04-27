package dataapi

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// oidTypeName maps well-known PostgreSQL type OIDs to the short type-name
// strings used throughout this package (makeScanDest, awsTypeCode, etc.).
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

// makeScanDest returns a pointer of the appropriate type for scanning a column.
// Using typed scan destinations gives pgx the information it needs to produce
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
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE":
		return new(sql.NullTime)
	case "TIME", "TIMETZ":
		// pgtype.Time is the native pgx type for OID 1083/1266; sql.NullTime
		// cannot receive the pgtype.Time struct that pgx decodes these to.
		return new(pgtype.Time)
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

	case *pgtype.Time:
		if !v.Valid {
			return NullField()
		}
		// Microseconds since midnight → time.Time for formatting.
		t := time.Time{}.Add(time.Duration(v.Microseconds) * time.Microsecond)
		return StringField(formatTimeValue(t, typeName))

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
func scanRowToFields(rows pgx.Rows, descs []pgconn.FieldDescription) ([]Field, error) {
	dests := make([]any, len(descs))
	for i, fd := range descs {
		dests[i] = makeScanDest(oidTypeName(fd.DataTypeOID))
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, fmt.Errorf("row scan: %w", err)
	}
	fields := make([]Field, len(descs))
	for i, dest := range dests {
		fields[i] = destToField(dest, oidTypeName(descs[i].DataTypeOID))
	}
	return fields, nil
}

// scanRowToMap scans the current row into a map[string]any for the
// formatRecordsAs=JSON path.
func scanRowToMap(rows pgx.Rows, descs []pgconn.FieldDescription) (map[string]any, error) {
	dests := make([]any, len(descs))
	for i, fd := range descs {
		dests[i] = makeScanDest(oidTypeName(fd.DataTypeOID))
	}
	if err := rows.Scan(dests...); err != nil {
		return nil, fmt.Errorf("row scan: %w", err)
	}
	row := make(map[string]any, len(descs))
	for i, fd := range descs {
		row[fd.Name] = destToNative(dests[i], oidTypeName(fd.DataTypeOID))
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
	case *pgtype.Time:
		if !v.Valid {
			return nil
		}
		t := time.Time{}.Add(time.Duration(v.Microseconds) * time.Microsecond)
		return formatTimeValue(t, typeName)
	case *sql.NullString:
		if !v.Valid {
			return nil
		}
		return v.String
	}
	return nil
}
