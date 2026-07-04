package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"notifier/internal/domain"
	"notifier/internal/observability"
	"notifier/internal/service"
)

// TemplateService is what the template handlers need.
type TemplateService interface {
	Create(ctx context.Context, input service.CreateTemplateInput) (domain.Template, error)
	GetByName(ctx context.Context, name string) (domain.Template, error)
	List(ctx context.Context) ([]domain.Template, error)
}

type templateHandler struct {
	templates TemplateService
	logger    *slog.Logger
}

type createTemplateRequest struct {
	Name    string `json:"name"`
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

type templateResponse struct {
	Name      string    `json:"name"`
	Channel   string    `json:"channel"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func toTemplateResponse(template domain.Template) templateResponse {
	return templateResponse{
		Name:      template.Name,
		Channel:   string(template.Channel),
		Body:      template.Body,
		CreatedAt: template.CreatedAt,
		UpdatedAt: template.UpdatedAt,
	}
}

func (handler *templateHandler) create(writer http.ResponseWriter, request *http.Request) {
	var payload createTemplateRequest
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeErrorResponse(writer, http.StatusBadRequest, "malformed JSON body", nil)
		return
	}

	created, err := handler.templates.Create(request.Context(), service.CreateTemplateInput{
		Name:    payload.Name,
		Channel: domain.Channel(payload.Channel),
		Body:    payload.Body,
	})
	if err != nil {
		var validationErrs domain.ValidationErrors
		switch {
		case errors.As(err, &validationErrs):
			writeErrorResponse(writer, http.StatusBadRequest, "validation failed", validationErrs)
		case errors.Is(err, domain.ErrDuplicateTemplateName):
			writeErrorResponse(writer, http.StatusConflict, "template name already exists", nil)
		default:
			observability.LoggerFrom(request.Context(), handler.logger).Error("create template failed", slog.Any("error", err))
			writeErrorResponse(writer, http.StatusInternalServerError, "internal error", nil)
		}
		return
	}

	writeJSONResponse(writer, http.StatusCreated, toTemplateResponse(created))
}

func (handler *templateHandler) get(writer http.ResponseWriter, request *http.Request) {
	name := chi.URLParam(request, "name")

	found, err := handler.templates.GetByName(request.Context(), name)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeErrorResponse(writer, http.StatusNotFound, "template not found", nil)
			return
		}
		observability.LoggerFrom(request.Context(), handler.logger).Error("get template failed", slog.Any("error", err))
		writeErrorResponse(writer, http.StatusInternalServerError, "internal error", nil)
		return
	}

	writeJSONResponse(writer, http.StatusOK, toTemplateResponse(found))
}

func (handler *templateHandler) list(writer http.ResponseWriter, request *http.Request) {
	templates, err := handler.templates.List(request.Context())
	if err != nil {
		observability.LoggerFrom(request.Context(), handler.logger).Error("list templates failed", slog.Any("error", err))
		writeErrorResponse(writer, http.StatusInternalServerError, "internal error", nil)
		return
	}

	responses := make([]templateResponse, len(templates))
	for i, template := range templates {
		responses[i] = toTemplateResponse(template)
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"data": responses})
}
