package service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
)

type fakeBatchRepository struct {
	*fakeRepository
	batches [][]domain.Notification
}

func (repo *fakeBatchRepository) CreateBatch(_ context.Context, notifications []domain.Notification) error {
	repo.batches = append(repo.batches, notifications)
	for _, notification := range notifications {
		repo.stored[notification.ID] = notification
	}
	return nil
}

func (repo *fakeBatchRepository) ExistingIdempotencyKeys(_ context.Context, keys []string) (map[string]uuid.UUID, error) {
	existing := map[string]uuid.UUID{}
	for _, key := range keys {
		for _, notification := range repo.stored {
			if notification.IdempotencyKey != nil && *notification.IdempotencyKey == key {
				existing[key] = notification.ID
			}
		}
	}
	return existing, nil
}

func validBatchInput(content string) CreateInput {
	return CreateInput{Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: content}
}

func TestCreateBatchPartialSuccess(t *testing.T) {
	repo := newFakeRepository()
	batchRepo := &fakeBatchRepository{fakeRepository: repo}
	publisher := &fakePublisher{}
	svc := newTestServiceWithPublisher(repo, publisher)

	duplicateKey := "seen-before"
	repo.stored[uuid.New()] = domain.Notification{
		ID: uuid.New(), IdempotencyKey: &duplicateKey, Status: domain.StatusSent,
	}

	inputs := []CreateInput{
		validBatchInput("ok one"),
		{Recipient: "not-a-phone", Channel: domain.ChannelSMS, Content: "bad"},
		{Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "dup", IdempotencyKey: &duplicateKey},
		validBatchInput("ok two"),
	}

	result, err := svc.CreateBatch(context.Background(), batchRepo, inputs)
	if err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	if result.Accepted != 2 || result.Rejected != 2 {
		t.Errorf("accepted/rejected = %d/%d, want 2/2", result.Accepted, result.Rejected)
	}
	if result.Results[0].Status != BatchItemAccepted || result.Results[3].Status != BatchItemAccepted {
		t.Error("valid items not accepted")
	}
	if result.Results[1].Status != BatchItemRejected || len(result.Results[1].Errors) == 0 {
		t.Error("invalid item not rejected with field errors")
	}
	if result.Results[2].Status != BatchItemDuplicate || result.Results[2].ExistingID == nil {
		t.Error("duplicate key item not reported with existing ID")
	}

	if len(batchRepo.batches) != 1 || len(batchRepo.batches[0]) != 2 {
		t.Fatalf("inserted batches = %+v, want one batch of 2", batchRepo.batches)
	}
	for _, notification := range batchRepo.batches[0] {
		if notification.BatchID == nil || *notification.BatchID != result.BatchID {
			t.Error("inserted notification missing shared batch id")
		}
	}
	if len(publisher.published) != 2 {
		t.Errorf("published %d, want 2", len(publisher.published))
	}
	for _, itemResult := range result.Results {
		if itemResult.Status == BatchItemAccepted && itemResult.Notification.Status != domain.StatusQueued {
			t.Errorf("accepted item status = %s, want queued after publish", itemResult.Notification.Status)
		}
	}
}

func TestCreateBatchScheduledItemsNotPublished(t *testing.T) {
	repo := newFakeRepository()
	batchRepo := &fakeBatchRepository{fakeRepository: repo}
	publisher := &fakePublisher{}
	svc := newTestServiceWithPublisher(repo, publisher)

	future := testNow.Add(time.Hour)
	scheduled := validBatchInput("later")
	scheduled.ScheduledAt = &future

	result, err := svc.CreateBatch(context.Background(), batchRepo, []CreateInput{scheduled})
	if err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	if result.Accepted != 1 {
		t.Fatalf("accepted = %d, want 1", result.Accepted)
	}
	if len(publisher.published) != 0 {
		t.Error("scheduled batch item was published immediately")
	}
	if result.Results[0].Notification.Status != domain.StatusScheduled {
		t.Errorf("status = %s, want scheduled", result.Results[0].Notification.Status)
	}
}
