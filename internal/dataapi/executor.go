package dataapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// Querier abstracts the subset of *sql.DB / *sql.Tx that the executor needs,
// allowing Execute and BatchExecute to work transparently in both auto-commit
// and transactional contexts.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Execute runs a single SQL statement and returns the Data API response.
func Execute(ctx context.Context, q Querier, req *ExecuteStatementRequest) (*ExecuteStatementResponse, error) {
	paramMap, err := buildParamMap(req.Parameters)
	if err != nil {
		return nil, err
	}

	rewritten, args, err := ParseNamedParams(req.SQL, paramMap)
	if err != nil {
		return nil, err
	}

	rows, err := q.QueryContext(ctx, rewritten, args...)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	resp := &ExecuteStatementResponse{}

	if len(colTypes) == 0 {
		// DML or DDL — exhaust the iterator then re-run via ExecContext to get
		// RowsAffected, which database/sql does not expose from QueryContext.
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		rows.Close()

		result, execErr := q.ExecContext(ctx, rewritten, args...)
		if execErr != nil {
			return nil, fmt.Errorf("%w", execErr)
		}
		n, _ := result.RowsAffected()
		resp.NumberOfRecordsUpdated = n
		resp.GeneratedFields = []Field{}
		return resp, nil
	}

	// SELECT / RETURNING path
	if req.IncludeResultMetadata {
		meta := make([]ColumnMetadata, len(colTypes))
		for i, ct := range colTypes {
			meta[i] = columnMetadataFromType(ct)
		}
		resp.ColumnMetadata = meta
	}

	if req.FormatRecordsAs == "JSON" {
		rowMaps := make([]map[string]any, 0)
		for rows.Next() {
			m, err := scanRowToMap(rows, colTypes)
			if err != nil {
				return nil, err
			}
			rowMaps = append(rowMaps, m)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		b, err := json.Marshal(rowMaps)
		if err != nil {
			return nil, fmt.Errorf("marshal formattedRecords: %w", err)
		}
		resp.FormattedRecords = string(b)
	} else {
		records := make([][]Field, 0)
		for rows.Next() {
			fields, err := scanRowToFields(rows, colTypes)
			if err != nil {
				return nil, err
			}
			records = append(records, fields)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		resp.Records = records
	}

	return resp, nil
}

// BatchExecute runs a SQL statement once per parameter set and collects the
// generated fields from each execution.
func BatchExecute(ctx context.Context, q Querier, req *BatchExecuteStatementRequest) (*BatchExecuteStatementResponse, error) {
	results := make([]UpdateResult, 0, len(req.ParameterSets))

	for _, paramSet := range req.ParameterSets {
		paramMap, err := buildParamMap(paramSet)
		if err != nil {
			return nil, err
		}

		rewritten, args, err := ParseNamedParams(req.SQL, paramMap)
		if err != nil {
			return nil, err
		}

		rows, err := q.QueryContext(ctx, rewritten, args...)
		if err != nil {
			return nil, fmt.Errorf("%w", err)
		}

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("column types: %w", err)
		}

		var genFields []Field
		if len(colTypes) > 0 {
			for rows.Next() {
				fields, err := scanRowToFields(rows, colTypes)
				if err != nil {
					rows.Close()
					return nil, err
				}
				genFields = append(genFields, fields...)
			}
		} else {
			for rows.Next() {
			}
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, err
		}

		if genFields == nil {
			genFields = []Field{}
		}
		results = append(results, UpdateResult{GeneratedFields: genFields})
	}

	return &BatchExecuteStatementResponse{UpdateResults: results}, nil
}

// buildParamMap converts a []SqlParameter into the map[string]interface{} that
// ParseNamedParams expects, applying any typeHint conversions.
func buildParamMap(params []SQLParameter) (map[string]any, error) {
	if len(params) == 0 {
		return nil, nil
	}
	m := make(map[string]any, len(params))
	for _, p := range params {
		val, err := p.Value.Value()
		if err != nil {
			return nil, err
		}
		if str, ok := val.(string); ok && p.TypeHint != "" {
			converted, err := applyTypeHint(str, p.TypeHint)
			if err != nil {
				return nil, fmt.Errorf("typeHint %q for parameter %q: %w", p.TypeHint, p.Name, err)
			}
			val = converted
		}
		m[p.Name] = val
	}
	return m, nil
}

// applyTypeHint converts a string parameter value according to the AWS Data API
// typeHint. Where possible we return a typed Go value so pgx can bind it with
// the correct PostgreSQL OID rather than as untyped text.
func applyTypeHint(s, hint string) (any, error) {
	switch hint {
	case "DATE":
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			return nil, fmt.Errorf("invalid DATE value %q: %w", s, err)
		}
		return t, nil

	case "TIMESTAMP":
		formats := []string{
			"2006-01-02 15:04:05.999999999",
			"2006-01-02T15:04:05.999999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
		}
		for _, f := range formats {
			if t, err := time.Parse(f, s); err == nil {
				return t, nil
			}
		}
		return s, nil

	default:
		return s, nil
	}
}
