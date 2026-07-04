package api

import (
	"encoding/json"
	"net/http"

	"notifier/internal/domain"
)

type fieldErrorResponse struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

type errorResponse struct {
	Error  string               `json:"error"`
	Fields []fieldErrorResponse `json:"fields,omitempty"`
}

func writeJSONResponse(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	// Encoding a value we constructed cannot fail; the client may have
	// gone away, which is not actionable here.
	_ = json.NewEncoder(writer).Encode(body)
}

func writeErrorResponse(writer http.ResponseWriter, status int, message string, validationErrs domain.ValidationErrors) {
	response := errorResponse{Error: message}
	for _, fieldErr := range validationErrs {
		response.Fields = append(response.Fields, fieldErrorResponse{
			Field:   fieldErr.Field,
			Message: fieldErr.Message,
		})
	}
	writeJSONResponse(writer, status, response)
}
