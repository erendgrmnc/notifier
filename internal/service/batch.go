package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/observability"
)

// MaxBatchSize is the default batch ceiling (the assessment's spec);
// deployments tune it with MAX_BATCH_SIZE.
const MaxBatchSize = 1000

// BatchRepository is the additional persistence surface batch creation
// needs beyond Repository.
type BatchRepository interface {
	CreateBatch(ctx context.Context, notifications []domain.Notification) error
	ExistingIdempotencyKeys(ctx context.Context, keys []string) (map[string]uuid.UUID, error)
	MarkQueuedBulk(ctx context.Context, ids []uuid.UUID) error
}

// BatchItemStatus classifies one item's outcome in a batch request.
type BatchItemStatus string

const (
	BatchItemAccepted  BatchItemStatus = "accepted"
	BatchItemRejected  BatchItemStatus = "rejected"
	BatchItemDuplicate BatchItemStatus = "duplicate"
)

// BatchItemResult reports one input item's outcome, keyed by its index
// in the request so partial success is unambiguous.
type BatchItemResult struct {
	Index        int
	Status       BatchItemStatus
	Notification *domain.Notification
	// ExistingID points at the earlier notification a duplicate
	// idempotency key collided with.
	ExistingID *uuid.UUID
	Errors     domain.ValidationErrors
}

// BatchResult is the whole batch outcome.
type BatchResult struct {
	BatchID  uuid.UUID
	Accepted int
	Rejected int
	Results  []BatchItemResult
}

// CreateBatch validates every item, inserts the valid ones in a single
// transaction under one batch ID, and publishes them for delivery after
// commit. Invalid or duplicate items are reported per index; valid ones
// proceed (partial success).
func (svc *NotificationService) CreateBatch(ctx context.Context, inputs []CreateInput) (BatchResult, error) {
	now := svc.clock.Now()
	batchID := uuid.New()
	result := BatchResult{BatchID: batchID, Results: make([]BatchItemResult, len(inputs))}

	// Pre-check idempotency keys in one query. Races between the check
	// and the COPY roll back the whole batch (single unique constraint),
	// which is honest: the caller retries and gets per-item duplicates.
	existingKeys, err := svc.existingKeysFor(ctx, inputs)
	if err != nil {
		return BatchResult{}, err
	}

	// One resolver per batch: each referenced template is fetched and
	// compiled once regardless of how many items use it.
	resolver := newTemplateResolver(svc.templateRepo)

	var toInsert []domain.Notification
	for index, input := range inputs {
		itemResult := BatchItemResult{Index: index, Status: BatchItemAccepted}

		content, err := svc.resolveContent(ctx, resolver, input)
		if err != nil {
			var validationErrs domain.ValidationErrors
			if !errors.As(err, &validationErrs) {
				return BatchResult{}, fmt.Errorf("resolve batch item %d content: %w", index, err)
			}
			itemResult.Status = BatchItemRejected
			itemResult.Errors = validationErrs
			result.Results[index] = itemResult
			result.Rejected++
			continue
		}

		notification := newNotification(input, content, now, &batchID)

		if input.IdempotencyKey != nil {
			if existingID, duplicate := existingKeys[*input.IdempotencyKey]; duplicate {
				itemResult.Status = BatchItemDuplicate
				if existingID != uuid.Nil {
					itemResult.ExistingID = &existingID
				}
				result.Results[index] = itemResult
				result.Rejected++
				continue
			}
			// Track keys within this request too: two items sharing a new
			// key would otherwise both enter the COPY and fail the whole
			// batch with a unique violation.
			existingKeys[*input.IdempotencyKey] = notification.ID
		}

		if err := domain.ValidateNew(notification, now); err != nil {
			var validationErrs domain.ValidationErrors
			if !errors.As(err, &validationErrs) {
				return BatchResult{}, fmt.Errorf("validate batch item %d: %w", index, err)
			}
			itemResult.Status = BatchItemRejected
			itemResult.Errors = validationErrs
			result.Results[index] = itemResult
			result.Rejected++
			continue
		}

		itemResult.Notification = &notification
		result.Results[index] = itemResult
		toInsert = append(toInsert, notification)
		result.Accepted++
	}

	if err := svc.batchRepo.CreateBatch(ctx, toInsert); err != nil {
		return BatchResult{}, fmt.Errorf("insert batch: %w", err)
	}
	for _, notification := range toInsert {
		svc.recordCreated(notification)
	}

	// Publish after commit, never inside the transaction — a rollback
	// must not leave phantom messages. The whole burst publishes under
	// one lock with one collective confirm wait, then flips to queued in
	// a single UPDATE; unconfirmed rows stay pending for the sweeper.
	var toPublish []domain.Notification
	for index := range result.Results {
		itemResult := &result.Results[index]
		if itemResult.Status == BatchItemAccepted && itemResult.Notification.Status == domain.StatusPending {
			toPublish = append(toPublish, *itemResult.Notification)
		}
	}

	confirmed, err := svc.publisher.PublishCreatedAll(ctx, toPublish)
	if err != nil {
		observability.LoggerFrom(ctx, svc.logger).Warn("batch publish failed; rows stay pending for sweeper", slog.Any("error", err))
		return result, nil
	}
	if err := svc.batchRepo.MarkQueuedBulk(ctx, confirmed); err != nil {
		observability.LoggerFrom(ctx, svc.logger).Warn("bulk queued mark failed", slog.Any("error", err))
		return result, nil
	}

	confirmedSet := make(map[uuid.UUID]struct{}, len(confirmed))
	for _, id := range confirmed {
		confirmedSet[id] = struct{}{}
	}
	for index := range result.Results {
		itemResult := &result.Results[index]
		if itemResult.Notification == nil {
			continue
		}
		if _, ok := confirmedSet[itemResult.Notification.ID]; ok {
			itemResult.Notification.Status = domain.StatusQueued
			svc.emitEvent(ctx, *itemResult.Notification, domain.StatusQueued)
		}
	}

	return result, nil
}

func (svc *NotificationService) existingKeysFor(ctx context.Context, inputs []CreateInput) (map[string]uuid.UUID, error) {
	var keys []string
	for _, input := range inputs {
		if input.IdempotencyKey != nil {
			keys = append(keys, *input.IdempotencyKey)
		}
	}
	existing, err := svc.batchRepo.ExistingIdempotencyKeys(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("check batch idempotency keys: %w", err)
	}
	return existing, nil
}
