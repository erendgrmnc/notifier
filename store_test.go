package main

import (
	"net/http"
	"net/http/httptest"
	"notifier/notification"
	"strings"
	"testing"
)

// Write your tests here.
//
// Go test idiom: name functions func TestXxx(t *testing.T), add `import "testing"`,
// then run `go test ./...`. Good first tests: Save then Get returns it; Get of a
// missing id returns ok==false.

func resetTestState() {
	notificationStorage = notification.New()
	notifciationChannel = make(chan int, 10)
}

func TestEnqueue(t *testing.T) {
	resetTestState()

	body := strings.NewReader(`{"content":"test notification"}`)

	req := httptest.NewRequest(http.MethodPost, "/notifications", body)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()

	enqueue(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got: %d", rec.Code)
	}

	select {
	case id := <-notifciationChannel:
		if id != 0 {
			t.Fatalf("expected id 0, got: %d", id)
		}
	default:
		t.Fatal("expected notification id to pushed to channel")
	}
}

func TestDequeue(t *testing.T) {
	resetTestState()

	body := strings.NewReader(`{"content":"test notification"}`)

	req := httptest.NewRequest(http.MethodPost, "/notifications", body)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	enqueue(rec, req)

	bodyDeq := strings.NewReader("")

	deqReq := httptest.NewRequest(http.MethodGet, "/notifications/0", bodyDeq)
	deqReq.Header.Set("Content-Type", "application/json")

	deqRec := httptest.NewRecorder()

	dequeue(deqRec, deqReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("Expected status 200, received: %d", rec.Code)
	}

	notification, isNotificationExists := notification.Get(0, notificationStorage)

	if !isNotificationExists {
		t.Fatalf("Notification is not registered")
	}

	if notification.Status != "queued" {
		t.Fatalf("Notification not registered properly, status is: %s", notification.Status)
	}
}
