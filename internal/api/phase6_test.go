package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/service"
)

func TestCreateReplayReturns200(t *testing.T) {
	fake := newFakeNotificationService()
	fake.replayNext = true
	router := newRouterWithService(fake)

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","idempotency_key":"key-1"}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body)))

	if recorder.Code != http.StatusOK {
		t.Fatalf("replayed create = %d, want 200", recorder.Code)
	}
}

func TestCancelEndpointStatusCodes(t *testing.T) {
	fake := newFakeNotificationService()
	router := newRouterWithService(fake)

	pending := domain.Notification{ID: uuid.New(), Status: domain.StatusPending}
	sent := domain.Notification{ID: uuid.New(), Status: domain.StatusSent}
	fake.stored[pending.ID] = pending
	fake.stored[sent.ID] = sent

	testCases := []struct {
		name string
		path string
		want int
	}{
		{name: "cancellable", path: "/api/v1/notifications/" + pending.ID.String() + "/cancel", want: http.StatusOK},
		{name: "terminal conflict", path: "/api/v1/notifications/" + sent.ID.String() + "/cancel", want: http.StatusConflict},
		{name: "unknown", path: "/api/v1/notifications/" + uuid.NewString() + "/cancel", want: http.StatusNotFound},
		{name: "malformed id", path: "/api/v1/notifications/nope/cancel", want: http.StatusBadRequest},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, tc.path, nil))
			if recorder.Code != tc.want {
				t.Errorf("status = %d, want %d", recorder.Code, tc.want)
			}
		})
	}
}

func TestBatchEndpointReturnsPerItemResults(t *testing.T) {
	fake := newFakeNotificationService()
	router := newRouterWithService(fake)

	body := `{"notifications":[
		{"recipient":"+905551111111","channel":"sms","content":"one"},
		{"recipient":"+905552222222","channel":"sms","content":"two"}
	]}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(body)))

	if recorder.Code != http.StatusCreated {
		t.Fatalf("batch = %d, want 201; body %s", recorder.Code, recorder.Body.String())
	}
	var response batchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal batch response: %v", err)
	}
	if response.Accepted != 2 || len(response.Results) != 2 {
		t.Errorf("accepted=%d results=%d, want 2/2", response.Accepted, len(response.Results))
	}
	if response.BatchID == uuid.Nil {
		t.Error("batch_id missing")
	}
}

func TestBatchEndpointRejectsOversizedBatch(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	var items []string
	for i := 0; i < service.MaxBatchSize+1; i++ {
		items = append(items, fmt.Sprintf(`{"recipient":"+90555%07d","channel":"sms","content":"x"}`, i))
	}
	body := `{"notifications":[` + strings.Join(items, ",") + `]}`

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(body)))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("oversized batch = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "exceeds") {
		t.Errorf("error body does not mention the limit: %s", recorder.Body.String())
	}
}

func TestBatchEndpointRejectsEmptyBatch(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications/batch", strings.NewReader(`{"notifications":[]}`)))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("empty batch = %d, want 400", recorder.Code)
	}
}

func TestListPassesFiltersAndPaginates(t *testing.T) {
	fake := newFakeNotificationService()
	router := newRouterWithService(fake)

	// Exactly `limit` stored rows → next_cursor must be present.
	for i := 0; i < 2; i++ {
		if _, err := fake.Create(context.Background(), service.CreateInput{
			Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: fmt.Sprintf("n%d", i),
			Priority: domain.PriorityNormal,
		}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=2&status=pending&channel=sms", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("list = %d, want 200; body %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data       []json.RawMessage `json:"data"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(response.Data) != 2 {
		t.Errorf("data = %d rows, want 2", len(response.Data))
	}
	if response.NextCursor == "" {
		t.Error("full page returned no next_cursor")
	}

	cursor, err := decodeCursor(response.NextCursor)
	if err != nil {
		t.Fatalf("returned cursor does not decode: %v", err)
	}
	if cursor.ID == uuid.Nil || cursor.CreatedAt.IsZero() {
		t.Error("decoded cursor missing keyset fields")
	}
}

func TestListRejectsMalformedParams(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	for _, path := range []string{
		"/api/v1/notifications?cursor=%%%",
		"/api/v1/notifications?batch_id=nope",
		"/api/v1/notifications?from=yesterday",
	} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("GET %s = %d, want 400", path, recorder.Code)
		}
	}
}

func TestCursorRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 7, 4, 12, 0, 0, 123456789, time.UTC)
	id := uuid.New()

	decoded, err := decodeCursor(encodeCursor(createdAt, id))
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !decoded.CreatedAt.Equal(createdAt) || decoded.ID != id {
		t.Errorf("round trip = (%v, %s), want (%v, %s)", decoded.CreatedAt, decoded.ID, createdAt, id)
	}
}
