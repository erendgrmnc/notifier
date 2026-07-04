package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"notifier/internal/domain"
)

func testNotification() domain.Notification {
	return domain.Notification{
		Recipient: "+905551234567",
		Channel:   domain.ChannelSMS,
		Content:   "hello",
	}
}

func TestWebhookSenderSuccess(t *testing.T) {
	var received map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Errorf("decode provider request: %v", err)
		}
		writer.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(writer).Encode(map[string]string{
			"messageId": "provider-123",
			"status":    "accepted",
			"timestamp": "2026-07-04T12:00:00Z",
		})
	}))
	defer server.Close()

	sender := NewWebhookSender(server.URL, time.Second)
	sendResult, err := sender.Send(context.Background(), testNotification())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	if sendResult.ProviderMessageID != "provider-123" {
		t.Errorf("ProviderMessageID = %q, want provider-123", sendResult.ProviderMessageID)
	}
	if sendResult.Body == "" || sendResult.StatusCode == 0 {
		t.Errorf("Result = %+v, want captured status and body snapshot", sendResult)
	}
	if received["to"] != "+905551234567" || received["channel"] != "sms" || received["content"] != "hello" {
		t.Errorf("provider received %v, want to/channel/content fields", received)
	}
}

func TestWebhookSenderClassification(t *testing.T) {
	testCases := []struct {
		name          string
		status        int
		wantRetryable bool
	}{
		{name: "500 retryable", status: http.StatusInternalServerError, wantRetryable: true},
		{name: "503 retryable", status: http.StatusServiceUnavailable, wantRetryable: true},
		{name: "429 retryable", status: http.StatusTooManyRequests, wantRetryable: true},
		{name: "400 permanent", status: http.StatusBadRequest, wantRetryable: false},
		{name: "404 permanent", status: http.StatusNotFound, wantRetryable: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(tc.status)
			}))
			defer server.Close()

			sender := NewWebhookSender(server.URL, time.Second)
			_, err := sender.Send(context.Background(), testNotification())
			if err == nil {
				t.Fatalf("Send returned nil for status %d", tc.status)
			}

			var sendErr *SendError
			if !errors.As(err, &sendErr) {
				t.Fatalf("error type %T, want *SendError", err)
			}
			if sendErr.Retryable != tc.wantRetryable {
				t.Errorf("Retryable = %v, want %v", sendErr.Retryable, tc.wantRetryable)
			}
			if sendErr.StatusCode != tc.status {
				t.Errorf("StatusCode = %d, want %d", sendErr.StatusCode, tc.status)
			}
		})
	}
}

func TestWebhookSenderNetworkErrorIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Close() // connection refused from now on

	sender := NewWebhookSender(server.URL, time.Second)
	_, err := sender.Send(context.Background(), testNotification())
	if err == nil {
		t.Fatal("Send returned nil against a closed server")
	}
	if !IsRetryable(err) {
		t.Errorf("network error classified permanent: %v", err)
	}
}

func TestWebhookSenderAcceptsMissingMessageID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK) // webhook.site default: 200, non-JSON body
	}))
	defer server.Close()

	sender := NewWebhookSender(server.URL, time.Second)
	sendResult, err := sender.Send(context.Background(), testNotification())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if sendResult.ProviderMessageID != "" {
		t.Errorf("ProviderMessageID = %q, want empty for bodyless 2xx", sendResult.ProviderMessageID)
	}
}

func TestIsRetryableDefaultsTrueForUnknownErrors(t *testing.T) {
	if !IsRetryable(errors.New("something unexpected")) {
		t.Error("unknown error classified permanent; safe default is retryable")
	}
}
