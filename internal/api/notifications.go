package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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
	Create(ctx context.Context, input service.CreateInput) (service.CreateResult, error)
	CreateBatch(ctx context.Context, inputs []service.CreateInput) (service.BatchResult, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	List(ctx context.Context, query domain.ListQuery) ([]domain.Notification, error)
}

type notificationHandler struct {
	notifications NotificationService
	logger        *slog.Logger
}

type templateRefRequest struct {
	Name string            `json:"name"`
	Vars map[string]string `json:"vars"`
}

type createNotificationRequest struct {
	Recipient      string              `json:"recipient"`
	Channel        string              `json:"channel"`
	Content        string              `json:"content"`
	Template       *templateRefRequest `json:"template,omitempty"`
	Priority       string              `json:"priority,omitempty"`
	ScheduledAt    *time.Time          `json:"scheduled_at,omitempty"`
	IdempotencyKey *string             `json:"idempotency_key,omitempty"`
}

func (payload createNotificationRequest) toCreateInput() service.CreateInput {
	input := service.CreateInput{
		Recipient:      payload.Recipient,
		Channel:        domain.Channel(payload.Channel),
		Content:        payload.Content,
		Priority:       domain.Priority(payload.Priority),
		ScheduledAt:    payload.ScheduledAt,
		IdempotencyKey: payload.IdempotencyKey,
	}
	if payload.Template != nil {
		input.Template = &service.TemplateRef{Name: payload.Template.Name, Vars: payload.Template.Vars}
	}
	return input
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
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErrorResponse(writer, http.StatusRequestEntityTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit), nil)
			return
		}
		writeErrorResponse(writer, http.StatusBadRequest, "malformed JSON body", nil)
		return
	}

	result, err := handler.notifications.Create(request.Context(), payload.toCreateInput())
	if err != nil {
		handler.writeServiceError(writer, request, err)
		return
	}

	// An idempotent replay returns the original resource with 200: the
	// client's retry succeeded without creating anything new.
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	writeJSONResponse(writer, status, toNotificationResponse(result.Notification))
}

// list returns filtered notifications newest-first as {data, next_cursor}.
func (handler *notificationHandler) list(writer http.ResponseWriter, request *http.Request) {
	listQuery, err := parseListQuery(request)
	if err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, err.Error(), nil)
		return
	}

	notifications, err := handler.notifications.List(request.Context(), listQuery)
	if err != nil {
		handler.writeServiceError(writer, request, err)
		return
	}

	responses := make([]notificationResponse, len(notifications))
	for i, notification := range notifications {
		responses[i] = toNotificationResponse(notification)
	}

	// A full page implies more may follow; the cursor resumes after the
	// last returned row. Limit here mirrors the service clamp.
	effectiveLimit := listQuery.Limit
	if effectiveLimit <= 0 {
		effectiveLimit = service.DefaultListLimit
	}
	if effectiveLimit > service.MaxListLimit {
		effectiveLimit = service.MaxListLimit
	}
	nextCursor := ""
	if len(notifications) == effectiveLimit {
		last := notifications[len(notifications)-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
	}

	writeJSONResponse(writer, http.StatusOK, map[string]any{"data": responses, "next_cursor": nextCursor})
}

// parseListQuery translates query parameters into a domain list query.
func parseListQuery(request *http.Request) (domain.ListQuery, error) {
	params := request.URL.Query()
	listQuery := domain.ListQuery{
		Status:  domain.Status(params.Get("status")),
		Channel: domain.Channel(params.Get("channel")),
	}

	if limitParam := params.Get("limit"); limitParam != "" {
		parsed, err := strconv.Atoi(limitParam)
		if err != nil || parsed < 1 {
			return domain.ListQuery{}, fmt.Errorf("limit must be a positive integer")
		}
		listQuery.Limit = parsed
	}
	if batchParam := params.Get("batch_id"); batchParam != "" {
		batchID, err := uuid.Parse(batchParam)
		if err != nil {
			return domain.ListQuery{}, fmt.Errorf("batch_id must be a UUID")
		}
		listQuery.BatchID = &batchID
	}
	for name, target := range map[string]**time.Time{"from": &listQuery.From, "to": &listQuery.To} {
		if timeParam := params.Get(name); timeParam != "" {
			parsed, err := time.Parse(time.RFC3339, timeParam)
			if err != nil {
				return domain.ListQuery{}, fmt.Errorf("%s must be RFC3339", name)
			}
			*target = &parsed
		}
	}
	if cursorParam := params.Get("cursor"); cursorParam != "" {
		cursor, err := decodeCursor(cursorParam)
		if err != nil {
			return domain.ListQuery{}, fmt.Errorf("invalid cursor")
		}
		listQuery.CursorCreatedAt = &cursor.CreatedAt
		listQuery.CursorID = &cursor.ID
	}
	return listQuery, nil
}

// cancel stops a pending/scheduled/queued notification.
func (handler *notificationHandler) cancel(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, "id must be a UUID", nil)
		return
	}

	cancelled, err := handler.notifications.Cancel(request.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidTransition) {
			writeErrorResponse(writer, http.StatusConflict, "notification is no longer cancellable", nil)
			return
		}
		handler.writeServiceError(writer, request, err)
		return
	}

	writeJSONResponse(writer, http.StatusOK, toNotificationResponse(cancelled))
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
