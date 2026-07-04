package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"notifier/internal/domain"
	"notifier/internal/service"
)

type fakeNotificationService struct {
	stored     map[uuid.UUID]domain.Notification
	createErr  error
	lastCreate service.CreateInput
}

func newFakeNotificationService() *fakeNotificationService {
	return &fakeNotificationService{stored: map[uuid.UUID]domain.Notification{}}
}

func (fake *fakeNotificationService) Create(_ context.Context, input service.CreateInput) (domain.Notification, error) {
	fake.lastCreate = input
	if fake.createErr != nil {
		return domain.Notification{}, fake.createErr
	}
	notification := domain.Notification{
		ID:        uuid.New(),
		Recipient: input.Recipient,
		Channel:   input.Channel,
		Content:   input.Content,
		Priority:  input.Priority,
		Status:    domain.StatusPending,
		CreatedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
	}
	fake.stored[notification.ID] = notification
	return notification, nil
}

func (fake *fakeNotificationService) Get(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	notification, ok := fake.stored[id]
	if !ok {
		return domain.Notification{}, domain.ErrNotFound
	}
	return notification, nil
}

func newRouterWithService(fake *fakeNotificationService) http.Handler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(RouterConfig{
		Logger:         logger,
		RequestTimeout: time.Second,
		Notifications:  fake,
	})
}

func TestCreateNotificationReturns201(t *testing.T) {
	fake := newFakeNotificationService()
	router := newRouterWithService(fake)

	body := `{"recipient":"+905551234567","channel":"sms","content":"hello","priority":"high"}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body)))

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response["status"] != string(domain.StatusPending) {
		t.Errorf("response status = %v, want pending", response["status"])
	}
	if response["id"] == "" {
		t.Error("response has no id")
	}
	if fake.lastCreate.Priority != domain.PriorityHigh {
		t.Errorf("service received priority %q, want high", fake.lastCreate.Priority)
	}
}

func TestCreateNotificationValidationFailureReturns400(t *testing.T) {
	fake := newFakeNotificationService()
	fake.createErr = domain.ValidationErrors{{Field: "recipient", Message: "must be an E.164 phone number"}}
	router := newRouterWithService(fake)

	body := `{"recipient":"bad","channel":"sms","content":"hello"}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader(body)))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "recipient") {
		t.Errorf("400 body does not name failing field: %s", recorder.Body.String())
	}
}

func TestCreateNotificationMalformedJSONReturns400(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", strings.NewReader("{not json")))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestGetNotificationReturnsResource(t *testing.T) {
	fake := newFakeNotificationService()
	router := newRouterWithService(fake)

	created, err := fake.Create(context.Background(), service.CreateInput{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
		Priority:  domain.PriorityNormal,
	})
	if err != nil {
		t.Fatalf("seed notification: %v", err)
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+created.ID.String(), nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response["id"] != created.ID.String() {
		t.Errorf("response id = %v, want %s", response["id"], created.ID)
	}
}

func TestGetNotificationUnknownIDReturns404(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/notifications/"+uuid.NewString(), nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}

func TestGetNotificationMalformedIDReturns400(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/notifications/not-a-uuid", nil))

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}
