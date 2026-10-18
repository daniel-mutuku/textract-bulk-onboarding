// Package httpx holds the small JSON helpers the HTTP handlers share.
package httpx

import (
	"encoding/json"
	"net/http"
)

// maxBodyBytes fits the largest request body: a complete call listing 10,000 parts.
const maxBodyBytes = 2 << 20

// Decode reads a JSON request body into v. On failure it writes a 400 and returns false.
func Decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		Error(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}
