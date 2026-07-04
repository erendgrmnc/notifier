package postgres

// Integration tests against a real PostgreSQL. Skipped unless
// TEST_DATABASE_URL is set (CI provides a service container; locally:
//
//	docker exec notification-system-postgres-1 psql -U notifier -c "CREATE DATABASE notifier_test"
//	TEST_DATABASE_URL="postgres://notifier:notifier@localhost:5432/notifier_test?sslmode=disable" go test ./internal/storage/postgres/
//
// These cover the guarded SQL that the unit-test fakes can only imitate:
// claim semantics, idempotency violations, keyset pagination, and the
// scheduler's recovery queries.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"notifier/internal/domain"
)

var (
	integrationPool     *pgxpool.Pool
	integrationPoolOnce sync.Once
)

func integrationRepo(t *testing.T) *NotificationRepository {
	t.Helper()
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL not set; skipping postgres integration tests")
	}

	integrationPoolOnce.Do(func() {
		if _, err := Migrate(databaseURL); err != nil {
			t.Fatalf("migrate test database: %v", err)
		}
		pool, err := Connect(context.Background(), databaseURL)
		if err != nil {
			t.Fatalf("connect test database: %v", err)
		}
		integrationPool = pool
	})

	if _, err := integrationPool.Exec(context.Background(),
		`TRUNCATE notifications, notification_templates`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return NewNotificationRepository(integrationPool)
}

func insertNotification(t *testing.T, repo *NotificationRepository, mutate func(*domain.Notification)) domain.Notification {
	t.Helper()
	notification := domain.Notification{
		ID:        uuid.New(),
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "integration",
		Priority:  domain.PriorityNormal,
		Status:    domain.StatusQueued,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if mutate != nil {
		mutate(&notification)
	}
	if err := repo.Create(context.Background(), notification); err != nil {
		t.Fatalf("seed notification: %v", err)
	}
	return notification
}

func TestIntegrationClaimGuardSemantics(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()
	claimable := domain.StatusesAllowedInto(domain.StatusProcessing)

	queued := insertNotification(t, repo, nil)

	claimed, err := repo.ClaimForProcessing(ctx, queued.ID, claimable...)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if claimed.Status != domain.StatusProcessing || claimed.Attempts != 1 {
		t.Errorf("claim = %s/%d attempts, want processing/1", claimed.Status, claimed.Attempts)
	}

	// A redelivered message must be rejected while processing.
	if _, err := repo.ClaimForProcessing(ctx, queued.ID, claimable...); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("second claim error = %v, want ErrInvalidTransition", err)
	}

	// Cancelled rows are never claimable.
	cancelled := insertNotification(t, repo, func(n *domain.Notification) { n.Status = domain.StatusCancelled })
	if _, err := repo.ClaimForProcessing(ctx, cancelled.ID, claimable...); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("cancelled claim error = %v, want ErrInvalidTransition", err)
	}

	if _, err := repo.ClaimForProcessing(ctx, uuid.New(), claimable...); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("missing claim error = %v, want ErrNotFound", err)
	}
}

func TestIntegrationOutcomeGuards(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()
	claimable := domain.StatusesAllowedInto(domain.StatusProcessing)

	notification := insertNotification(t, repo, nil)
	if _, err := repo.ClaimForProcessing(ctx, notification.ID, claimable...); err != nil {
		t.Fatalf("claim: %v", err)
	}

	sentAt := time.Now().UTC()
	if err := repo.MarkSent(ctx, notification.ID, "provider-1", sentAt); err != nil {
		t.Fatalf("MarkSent: %v", err)
	}
	// Terminal rows reject every further outcome.
	if err := repo.MarkFailed(ctx, notification.ID, "late failure"); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("MarkFailed on sent = %v, want ErrInvalidTransition", err)
	}

	final, err := repo.GetByID(ctx, notification.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if final.Status != domain.StatusSent || final.ProviderMessageID == nil || *final.ProviderMessageID != "provider-1" {
		t.Errorf("final = %+v, want sent with provider id", final)
	}
}

func TestIntegrationIdempotencyKeyViolation(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()
	key := "integration-key"

	insertNotification(t, repo, func(n *domain.Notification) { n.IdempotencyKey = &key })

	duplicate := domain.Notification{
		ID: uuid.New(), Recipient: "+905551234567", Channel: domain.ChannelSMS,
		Content: "dup", Priority: domain.PriorityNormal, Status: domain.StatusPending,
		IdempotencyKey: &key, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := repo.Create(ctx, duplicate); !errors.Is(err, domain.ErrDuplicateIdempotencyKey) {
		t.Fatalf("duplicate create error = %v, want ErrDuplicateIdempotencyKey", err)
	}

	found, err := repo.GetByIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if found.IdempotencyKey == nil || *found.IdempotencyKey != key {
		t.Errorf("replay lookup returned %+v", found)
	}
}

func TestIntegrationBatchInsertAndBulkQueue(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()

	batchID := uuid.New()
	var batch []domain.Notification
	for i := 0; i < 50; i++ {
		batch = append(batch, domain.Notification{
			ID: uuid.New(), BatchID: &batchID, Recipient: "+905551234567",
			Channel: domain.ChannelSMS, Content: fmt.Sprintf("batch %d", i),
			Priority: domain.PriorityNormal, Status: domain.StatusPending,
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		})
	}
	if err := repo.CreateBatch(ctx, batch); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}

	ids := make([]uuid.UUID, len(batch))
	for i, notification := range batch {
		ids[i] = notification.ID
	}
	if err := repo.MarkQueuedBulk(ctx, ids); err != nil {
		t.Fatalf("MarkQueuedBulk: %v", err)
	}

	queued, err := repo.List(ctx, domain.ListQuery{BatchID: &batchID, Status: domain.StatusQueued, Limit: 100})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(queued) != 50 {
		t.Errorf("queued = %d, want 50", len(queued))
	}
}

func TestIntegrationKeysetPagination(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < 25; i++ {
		insertNotification(t, repo, func(n *domain.Notification) {
			n.CreatedAt = base.Add(time.Duration(i) * time.Second)
			n.UpdatedAt = n.CreatedAt
		})
	}

	seen := map[uuid.UUID]bool{}
	query := domain.ListQuery{Limit: 10}
	pages := 0
	for {
		page, err := repo.List(ctx, query)
		if err != nil {
			t.Fatalf("List page %d: %v", pages, err)
		}
		if len(page) == 0 {
			break
		}
		pages++
		for _, notification := range page {
			if seen[notification.ID] {
				t.Fatalf("cursor pagination returned %s twice", notification.ID)
			}
			seen[notification.ID] = true
		}
		last := page[len(page)-1]
		query.CursorCreatedAt = &last.CreatedAt
		query.CursorID = &last.ID
		if len(page) < query.Limit {
			break
		}
	}
	if len(seen) != 25 || pages != 3 {
		t.Errorf("walked %d rows in %d pages, want 25 in 3", len(seen), pages)
	}
}

func TestIntegrationSchedulerQueries(t *testing.T) {
	repo := integrationRepo(t)
	ctx := context.Background()

	// Due scheduled row + stale pending row are both claimed to queued.
	due := insertNotification(t, repo, func(n *domain.Notification) {
		n.Status = domain.StatusScheduled
		past := time.Now().UTC().Add(-time.Minute)
		n.ScheduledAt = &past
	})
	stalePending := insertNotification(t, repo, func(n *domain.Notification) {
		n.Status = domain.StatusPending
		n.CreatedAt = time.Now().UTC().Add(-10 * time.Minute)
	})
	freshPending := insertNotification(t, repo, func(n *domain.Notification) {
		n.Status = domain.StatusPending
	})

	claimed, err := repo.ClaimDueForQueue(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("ClaimDueForQueue: %v", err)
	}
	claimedIDs := map[uuid.UUID]bool{}
	for _, notification := range claimed {
		claimedIDs[notification.ID] = true
		if notification.Status != domain.StatusQueued {
			t.Errorf("claimed row status = %s, want queued", notification.Status)
		}
	}
	if !claimedIDs[due.ID] || !claimedIDs[stalePending.ID] || claimedIDs[freshPending.ID] {
		t.Errorf("claimed set = %v; want due+stale, not fresh", claimedIDs)
	}

	// Stuck processing row is recovered to retrying.
	stuck := insertNotification(t, repo, func(n *domain.Notification) { n.Status = domain.StatusQueued })
	if _, err := repo.ClaimForProcessing(ctx, stuck.ID, domain.StatusQueued); err != nil {
		t.Fatalf("claim stuck: %v", err)
	}
	if _, err := integrationPool.Exec(ctx,
		`UPDATE notifications SET updated_at = now() - interval '10 minutes' WHERE id = $1`, stuck.ID); err != nil {
		t.Fatalf("age stuck row: %v", err)
	}

	recovered, err := repo.RecoverStaleProcessing(ctx, time.Minute, 100)
	if err != nil {
		t.Fatalf("RecoverStaleProcessing: %v", err)
	}
	if len(recovered) != 1 || recovered[0].ID != stuck.ID || recovered[0].Status != domain.StatusRetrying {
		t.Errorf("recovered = %+v, want the stuck row as retrying", recovered)
	}
}

func TestIntegrationTemplateRepository(t *testing.T) {
	integrationRepo(t) // migrate + truncate
	repo := NewTemplateRepository(integrationPool)
	ctx := context.Background()

	created := domain.Template{
		ID: uuid.New(), Name: "integration-otp", Channel: domain.ChannelSMS,
		Body: "Code {{.code}}", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}
	if err := repo.CreateTemplate(ctx, created); err != nil {
		t.Fatalf("CreateTemplate: %v", err)
	}
	duplicate := created
	duplicate.ID = uuid.New()
	if err := repo.CreateTemplate(ctx, duplicate); !errors.Is(err, domain.ErrDuplicateTemplateName) {
		t.Errorf("duplicate name error = %v, want ErrDuplicateTemplateName", err)
	}

	found, err := repo.GetTemplateByName(ctx, "integration-otp")
	if err != nil || found.Body != "Code {{.code}}" {
		t.Errorf("GetTemplateByName = %+v, %v", found, err)
	}
	if _, err := repo.GetTemplateByName(ctx, "missing"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("missing template error = %v, want ErrNotFound", err)
	}
}
