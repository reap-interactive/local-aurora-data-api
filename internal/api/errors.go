package api

import (
	"encoding/json"
	"net/http"
)

// ErrorCode is the AWS-compatible error classification string included in all
// error responses. Using a named type prevents raw string literals from being
// passed where a code is expected.
type ErrorCode string

const (
	ErrBadRequest    ErrorCode = "BadRequestException"
	ErrNotFound      ErrorCode = "NotFoundException"
	ErrInternalError ErrorCode = "InternalServerErrorException"
)

// APIError is a structured HTTP error that carries a status code, AWS error
// code, and message. Returning an *APIError from a handler method causes
// handle[T] to write the correct HTTP response automatically.
type APIError struct {
	Status  int
	Code    ErrorCode
	Message string
}

func (e *APIError) Error() string { return e.Message }

func badRequest(msg string) *APIError {
	return &APIError{http.StatusBadRequest, ErrBadRequest, msg}
}

func internalError(msg string) *APIError {
	return &APIError{http.StatusInternalServerError, ErrInternalError, msg}
}

// ErrorResponse is the JSON body returned for all error responses.
type ErrorResponse struct {
	Message string    `json:"message"`
	Code    ErrorCode `json:"code"`
}

// writeJSON writes v as a JSON response with the given HTTP status code.
// HTML escaping is explicitly disabled so that characters like <, >, and &
// are written as-is rather than being replaced with \u003c / \u003e / \u0026.
// This is important for the formattedRecords field, which embeds a JSON string
// that would otherwise be mangled before reaching the caller.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

// writeError writes a structured AWS-compatible error response.
func writeError(w http.ResponseWriter, status int, code ErrorCode, message string) {
	writeJSON(w, status, ErrorResponse{Message: message, Code: code})
}
