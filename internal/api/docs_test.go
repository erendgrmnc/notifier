package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAPISpecServed(t *testing.T) {
	router := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/openapi.yaml", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/openapi.yaml = %d, want 200", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "openapi: 3.0.3") {
		t.Error("response does not look like the OpenAPI spec")
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "yaml") {
		t.Errorf("Content-Type = %q, want a yaml type", contentType)
	}
}

func TestSwaggerUIServed(t *testing.T) {
	router := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/docs", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /docs = %d, want 200", recorder.Code)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "swagger-ui") {
		t.Error("docs page does not embed swagger-ui")
	}
	if !strings.Contains(body, "/api/v1/openapi.yaml") {
		t.Error("docs page does not point at the served spec")
	}
}
