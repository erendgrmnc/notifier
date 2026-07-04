package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"log/slog"
	"net/http"

	"notifier/internal/mockprovider"
	"notifier/internal/observability"
	"notifier/internal/queue/rabbit"
)

//go:embed dashboard.html
var dashboardPage []byte

func handleDashboard(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(dashboardPage)
}

// WorkerControl toggles and reads the shared worker pause flag.
type WorkerControl interface {
	WorkerPaused(ctx context.Context) (bool, error)
	SetWorkerPaused(ctx context.Context, paused bool) error
}

// QueueInspector reads queue depths.
type QueueInspector interface {
	QueueDepths(ctx context.Context) ([]rabbit.QueueDepth, error)
}

// dashboardHandler serves the testing dashboard's control endpoints.
type dashboardHandler struct {
	workerControl WorkerControl
	queues        QueueInspector
	providerStore *mockprovider.Store
	logger        *slog.Logger
}

type workerStateResponse struct {
	Paused bool `json:"paused"`
}

func (handler *dashboardHandler) getWorkerState(writer http.ResponseWriter, request *http.Request) {
	paused, err := handler.workerControl.WorkerPaused(request.Context())
	if err != nil {
		handler.writeInternalError(writer, request, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, workerStateResponse{Paused: paused})
}

func (handler *dashboardHandler) setWorkerState(writer http.ResponseWriter, request *http.Request) {
	var state workerStateResponse
	if err := json.NewDecoder(request.Body).Decode(&state); err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, "malformed JSON body", nil)
		return
	}

	if err := handler.workerControl.SetWorkerPaused(request.Context(), state.Paused); err != nil {
		handler.writeInternalError(writer, request, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, state)
}

func (handler *dashboardHandler) getQueueDepths(writer http.ResponseWriter, request *http.Request) {
	depths, err := handler.queues.QueueDepths(request.Context())
	if err != nil {
		handler.writeInternalError(writer, request, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"queues": depths})
}

func (handler *dashboardHandler) getProviderMessages(writer http.ResponseWriter, _ *http.Request) {
	writeJSONResponse(writer, http.StatusOK, map[string]any{"messages": handler.providerStore.Recent()})
}

func (handler *dashboardHandler) writeInternalError(writer http.ResponseWriter, request *http.Request, err error) {
	observability.LoggerFrom(request.Context(), handler.logger).Error("dashboard request failed",
		slog.String("path", request.URL.Path), slog.Any("error", err))
	writeErrorResponse(writer, http.StatusInternalServerError, "internal error", nil)
}
