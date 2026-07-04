package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/observability"
	"notifier/internal/service"
)

// maxRequestBodyBytes caps request bodies well above the largest legal
// payload (100KB email content) while blocking abusive ones.
const maxRequestBodyBytes = 1 << 20 // 1 MiB

// NotificationService is what the handlers need from the service layer.
type NotificationService interface {
	Create(ctx context.Context, input service.CreateInput) (domain.Notification, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
}

type notificationHandler struct {
	notifications NotificationService
	logger        *slog.Logger
}

type createNotificationRequest struct {
	Recipient      string     `json:"recipient"`
	Channel        string     `json:"channel"`
	Content        string     `json:"content"`
	Priority       string     `json:"priority,omitempty"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty"`
	IdempotencyKey *string    `json:"idempotency_key,omitempty"`
}

type notificationResponse struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batch_id,omitempty"`
	Recipient         string     `json:"recipient"`
	Channel           string     `json:"channel"`
	Content           string     `json:"content"`
	Priority          string     `json:"priority"`
	Status            string     `json:"status"`
	ScheduledAt       *time.Time `json:"scheduled_at,omitempty"`
	Attempts          int        `json:"attempts"`
	LastError         *string    `json:"last_error,omitempty"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SentAt            *time.Time `json:"sent_at,omitempty"`
}

func toNotificationResponse(notification domain.Notification) notificationResponse {
	return notificationResponse{
		ID:                notification.ID,
		BatchID:           notification.BatchID,
		Recipient:         notification.Recipient,
		Channel:           string(notification.Channel),
		Content:           notification.Content,
		Priority:          string(notification.Priority),
		Status:            string(notification.Status),
		ScheduledAt:       notification.ScheduledAt,
		Attempts:          notification.Attempts,
		LastError:         notification.LastError,
		ProviderMessageID: notification.ProviderMessageID,
		CreatedAt:         notification.CreatedAt,
		UpdatedAt:         notification.UpdatedAt,
		SentAt:            notification.SentAt,
	}
}

func (handler *notificationHandler) create(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBodyBytes)

	var payload createNotificationRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, "malformed JSON body", nil)
		return
	}

	created, err := handler.notifications.Create(request.Context(), service.CreateInput{
		Recipient:      payload.Recipient,
		Channel:        domain.Channel(payload.Channel),
		Content:        payload.Content,
		Priority:       domain.Priority(payload.Priority),
		ScheduledAt:    payload.ScheduledAt,
		IdempotencyKey: payload.IdempotencyKey,
	})
	if err != nil {
		handler.writeServiceError(writer, request, err)
		return
	}

	writeJSONResponse(writer, http.StatusCreated, toNotificationResponse(created))
}

func (handler *notificationHandler) get(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, "id must be a UUID", nil)
		return
	}

	notification, err := handler.notifications.Get(request.Context(), id)
	if err != nil {
		handler.writeServiceError(writer, request, err)
		return
	}

	writeJSONResponse(writer, http.StatusOK, toNotificationResponse(notification))
}

// writeServiceError maps service-layer errors onto HTTP responses.
func (handler *notificationHandler) writeServiceError(writer http.ResponseWriter, request *http.Request, err error) {
	var validationErrs domain.ValidationErrors
	switch {
	case errors.As(err, &validationErrs):
		writeErrorResponse(writer, http.StatusBadRequest, "validation failed", validationErrs)
	case errors.Is(err, domain.ErrNotFound):
		writeErrorResponse(writer, http.StatusNotFound, "notification not found", nil)
	default:
		observability.LoggerFrom(request.Context(), handler.logger).Error("request failed",
			slog.String("path", request.URL.Path), slog.Any("error", err))
		writeErrorResponse(writer, http.StatusInternalServerError, "internal error", nil)
	}
}
