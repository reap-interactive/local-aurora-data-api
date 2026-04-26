package dataapi

import (
	"fmt"
)

// Field is the AWS Data API union value type. Only one member should be set.
// BlobValue is a []byte which encoding/json automatically encodes/decodes as base64.
type Field struct {
	BlobValue    []byte      `json:"blobValue,omitempty"`
	BooleanValue *bool       `json:"booleanValue,omitempty"`
	DoubleValue  *float64    `json:"doubleValue,omitempty"`
	IsNull       *bool       `json:"isNull,omitempty"`
	LongValue    *int64      `json:"longValue,omitempty"`
	StringValue  *string     `json:"stringValue,omitempty"`
	ArrayValue   *ArrayValue `json:"arrayValue,omitempty"`
}

// NullField returns a Field representing a SQL NULL.
func NullField() Field {
	t := true
	return Field{IsNull: &t}
}

// StringField returns a Field wrapping a string value.
func StringField(s string) Field {
	return Field{StringValue: &s}
}

// LongField returns a Field wrapping an int64 value.
func LongField(v int64) Field {
	return Field{LongValue: &v}
}

// DoubleField returns a Field wrapping a float64 value.
func DoubleField(v float64) Field {
	return Field{DoubleValue: &v}
}

// BoolField returns a Field wrapping a bool value.
func BoolField(v bool) Field {
	return Field{BooleanValue: &v}
}

// BlobField returns a Field wrapping raw binary data.
func BlobField(b []byte) Field {
	return Field{BlobValue: b}
}

// Value extracts the concrete Go value from a Field for use as a SQL parameter.
// Returns an error if the field contains an array (not supported in Execute).
func (f Field) Value() (any, error) {
	if f.IsNull != nil && *f.IsNull {
		return nil, nil
	}
	if f.BlobValue != nil {
		return f.BlobValue, nil
	}
	if f.BooleanValue != nil {
		return *f.BooleanValue, nil
	}
	if f.DoubleValue != nil {
		return *f.DoubleValue, nil
	}
	if f.LongValue != nil {
		return *f.LongValue, nil
	}
	if f.StringValue != nil {
		return *f.StringValue, nil
	}
	if f.ArrayValue != nil {
		return nil, fmt.Errorf("array parameters are not supported")
	}
	return nil, nil
}

// ArrayValue is the union type for array fields.
type ArrayValue struct {
	ArrayValues   []ArrayValue `json:"arrayValues,omitempty"`
	BooleanValues []bool       `json:"booleanValues,omitempty"`
	DoubleValues  []float64    `json:"doubleValues,omitempty"`
	LongValues    []int64      `json:"longValues,omitempty"`
	StringValues  []string     `json:"stringValues,omitempty"`
}

// SQLParameter holds a named parameter and its typed value.
type SQLParameter struct {
	Name     string `json:"name"`
	TypeHint string `json:"typeHint,omitempty"`
	Value    Field  `json:"value"`
}

// ResultSetOptions controls result set formatting.
type ResultSetOptions struct {
	DecimalReturnType string `json:"decimalReturnType,omitempty"`
	LongReturnType    string `json:"longReturnType,omitempty"`
}

// ColumnMetadata describes a result column.
type ColumnMetadata struct {
	IsSigned   bool   `json:"isSigned,omitempty"`
	Label      string `json:"label,omitempty"`
	Name       string `json:"name,omitempty"`
	Nullable   int    `json:"nullable"`
	Precision  int    `json:"precision,omitempty"`
	Scale      int    `json:"scale,omitempty"`
	SchemaName string `json:"schemaName,omitempty"`
	TableName  string `json:"tableName,omitempty"`
	Type       int    `json:"type,omitempty"`
	TypeName   string `json:"typeName,omitempty"`
}

// UpdateResult holds the generated fields from a single DML statement in a batch.
type UpdateResult struct {
	GeneratedFields []Field `json:"generatedFields"`
}

// --- Request types ---

// BeginTransactionRequest starts a new transaction.
type BeginTransactionRequest struct {
	ResourceArn string `json:"resourceArn"`
	SecretArn   string `json:"secretArn"`
	Database    string `json:"database,omitempty"`
	Schema      string `json:"schema,omitempty"`
}

// CommitTransactionRequest commits an open transaction.
type CommitTransactionRequest struct {
	ResourceArn   string `json:"resourceArn"`
	SecretArn     string `json:"secretArn"`
	TransactionID string `json:"transactionId"`
}

// RollbackTransactionRequest rolls back an open transaction.
type RollbackTransactionRequest struct {
	ResourceArn   string `json:"resourceArn"`
	SecretArn     string `json:"secretArn"`
	TransactionID string `json:"transactionId"`
}

// ExecuteStatementRequest executes a single SQL statement.
type ExecuteStatementRequest struct {
	ResourceArn           string            `json:"resourceArn"`
	SecretArn             string            `json:"secretArn"`
	SQL                   string            `json:"sql"`
	Database              string            `json:"database,omitempty"`
	Schema                string            `json:"schema,omitempty"`
	Parameters            []SQLParameter    `json:"parameters,omitempty"`
	TransactionID         string            `json:"transactionId,omitempty"`
	IncludeResultMetadata bool              `json:"includeResultMetadata,omitempty"`
	FormatRecordsAs       string            `json:"formatRecordsAs,omitempty"`
	ContinueAfterTimeout  bool              `json:"continueAfterTimeout,omitempty"`
	ResultSetOptions      *ResultSetOptions `json:"resultSetOptions,omitempty"`
}

// BatchExecuteStatementRequest executes a SQL statement once per parameter set.
type BatchExecuteStatementRequest struct {
	ResourceArn   string           `json:"resourceArn"`
	SecretArn     string           `json:"secretArn"`
	SQL           string           `json:"sql"`
	Database      string           `json:"database,omitempty"`
	Schema        string           `json:"schema,omitempty"`
	ParameterSets [][]SQLParameter `json:"parameterSets,omitempty"`
	TransactionID string           `json:"transactionId,omitempty"`
}

// --- Response types ---

// BeginTransactionResponse returns the new transaction ID.
type BeginTransactionResponse struct {
	TransactionID string `json:"transactionId"`
}

// CommitTransactionResponse confirms the commit.
type CommitTransactionResponse struct {
	TransactionStatus string `json:"transactionStatus"`
}

// RollbackTransactionResponse confirms the rollback.
type RollbackTransactionResponse struct {
	TransactionStatus string `json:"transactionStatus"`
}

// ExecuteStatementResponse returns the results of a single SQL execution.
type ExecuteStatementResponse struct {
	NumberOfRecordsUpdated int64            `json:"numberOfRecordsUpdated"`
	GeneratedFields        []Field          `json:"generatedFields,omitempty"`
	Records                [][]Field        `json:"records,omitempty"`
	ColumnMetadata         []ColumnMetadata `json:"columnMetadata,omitempty"`
	FormattedRecords       string           `json:"formattedRecords,omitempty"`
}

// BatchExecuteStatementResponse returns the per-row results of a batch execution.
type BatchExecuteStatementResponse struct {
	UpdateResults []UpdateResult `json:"updateResults"`
}
