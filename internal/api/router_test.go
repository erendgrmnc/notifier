package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestRouter(t *testing.T) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(RouterConfig{Logger: logger, RequestTimeout: time.Second})
}

func TestHealthzReturnsOK(t *testing.T) {
	router := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want %d", recorder.Code, http.StatusOK)
	}
}

func TestCorrelationIDEchoedFromRequestHeader(t *testing.T) {
	router := newTestRouter(t)

	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set(correlationIDHeader, "client-supplied-id")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if got := recorder.Header().Get(correlationIDHeader); got != "client-supplied-id" {
		t.Errorf("%s = %q, want client-supplied-id", correlationIDHeader, got)
	}
}

func TestCorrelationIDMintedWhenAbsent(t *testing.T) {
	router := newTestRouter(t)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if recorder.Header().Get(correlationIDHeader) == "" {
		t.Errorf("%s missing on response, want minted ID", correlationIDHeader)
	}
}

func TestPanicRecoveredAsInternalServerError(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	router := NewRouter(RouterConfig{Logger: logger, RequestTimeout: time.Second})
	router.Get("/panic", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("panicking handler = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
}
