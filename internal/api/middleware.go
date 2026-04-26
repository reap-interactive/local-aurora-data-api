package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// ── generic handler wrapper ───────────────────────────────────────────────────

// handleJSON wraps a typed handler function, taking care of JSON decoding and error
// translation so that handler methods only need to contain business logic.
func handleJSON[T any](fn func(context.Context, T) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req T
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrBadRequest, "invalid request body: "+err.Error())
			return
		}

		resp, err := fn(r.Context(), req)
		if err != nil {
			var apiErr *APIError
			if errors.As(err, &apiErr) {
				writeError(w, apiErr.Status, apiErr.Code, apiErr.Message)
			} else {
				writeError(w, http.StatusBadRequest, ErrBadRequest, err.Error())
			}
			return
		}

		writeJSON(w, http.StatusOK, resp)
	}
}
