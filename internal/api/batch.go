package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/service"
)

// maxBatchBodyBytes fits MaxBatchSize items with generous content.
const maxBatchBodyBytes = 10 << 20 // 10 MiB

type batchRequest struct {
	Notifications []createNotificationRequest `json:"notifications"`
}

type batchItemResponse struct {
	Index        int                   `json:"index"`
	Status       string                `json:"status"`
	Notification *notificationResponse `json:"notification,omitempty"`
	ExistingID   *uuid.UUID            `json:"existing_id,omitempty"`
	Errors       []fieldErrorResponse  `json:"errors,omitempty"`
}

type batchResponse struct {
	BatchID  uuid.UUID           `json:"batch_id"`
	Accepted int                 `json:"accepted"`
	Rejected int                 `json:"rejected"`
	Results  []batchItemResponse `json:"results"`
}

// createBatch accepts up to service.MaxBatchSize notifications and
// creates the valid ones, reporting every item's outcome by index.
func (handler *notificationHandler) createBatch(writer http.ResponseWriter, request *http.Request) {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBatchBodyBytes)

	var payload batchRequest
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

	if len(payload.Notifications) == 0 {
		writeErrorResponse(writer, http.StatusBadRequest, "notifications must not be empty", nil)
		return
	}
	if len(payload.Notifications) > service.MaxBatchSize {
		writeErrorResponse(writer, http.StatusBadRequest,
			fmt.Sprintf("batch exceeds %d notifications", service.MaxBatchSize), nil)
		return
	}

	inputs := make([]service.CreateInput, len(payload.Notifications))
	for i, item := range payload.Notifications {
		inputs[i] = service.CreateInput{
			Recipient:      item.Recipient,
			Channel:        domain.Channel(item.Channel),
			Content:        item.Content,
			Priority:       domain.Priority(item.Priority),
			ScheduledAt:    item.ScheduledAt,
			IdempotencyKey: item.IdempotencyKey,
		}
	}

	started := time.Now()
	result, err := handler.notifications.CreateBatch(request.Context(), inputs)
	if err != nil {
		handler.writeServiceError(writer, request, err)
		return
	}
	_ = started // duration metrics arrive with the observability phase

	writeJSONResponse(writer, http.StatusCreated, toBatchResponse(result))
}

func toBatchResponse(result service.BatchResult) batchResponse {
	response := batchResponse{
		BatchID:  result.BatchID,
		Accepted: result.Accepted,
		Rejected: result.Rejected,
		Results:  make([]batchItemResponse, len(result.Results)),
	}
	for i, itemResult := range result.Results {
		item := batchItemResponse{
			Index:      itemResult.Index,
			Status:     string(itemResult.Status),
			ExistingID: itemResult.ExistingID,
		}
		if itemResult.Notification != nil {
			notificationBody := toNotificationResponse(*itemResult.Notification)
			item.Notification = &notificationBody
		}
		for _, fieldErr := range itemResult.Errors {
			item.Errors = append(item.Errors, fieldErrorResponse{Field: fieldErr.Field, Message: fieldErr.Message})
		}
		response.Results[i] = item
	}
	return response
}
