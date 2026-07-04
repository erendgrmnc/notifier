package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"notifier/internal/domain"
)

type fakeLookup struct{ url string }

func (lookup *fakeLookup) ProviderOverride(context.Context) (string, error) {
	return lookup.url, nil
}

type recordingSender struct{ sent int }

func (sender *recordingSender) Send(context.Context, domain.Notification) (string, error) {
	sender.sent++
	return "fallback-id", nil
}

func TestSwitchableSenderFallsBackWithoutOverride(t *testing.T) {
	fallback := &recordingSender{}
	sender := NewSwitchableSender(&fakeLookup{}, fallback, time.Second)

	providerMessageID, err := sender.Send(context.Background(), domain.Notification{})
	if err != nil || providerMessageID != "fallback-id" {
		t.Fatalf("Send = %q, %v; want fallback", providerMessageID, err)
	}
	if fallback.sent != 1 {
		t.Errorf("fallback sent = %d, want 1", fallback.sent)
	}
}

func TestSwitchableSenderFollowsOverride(t *testing.T) {
	var received int
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		received++
		writer.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(writer).Encode(map[string]string{"messageId": "override-id"})
	}))
	defer server.Close()

	fallback := &recordingSender{}
	lookup := &fakeLookup{url: server.URL}
	sender := NewSwitchableSender(lookup, fallback, time.Second)

	providerMessageID, err := sender.Send(context.Background(), domain.Notification{
		Recipient: "+905551234567", Channel: domain.ChannelSMS, Content: "x",
	})
	if err != nil || providerMessageID != "override-id" {
		t.Fatalf("Send = %q, %v; want override target", providerMessageID, err)
	}
	if fallback.sent != 0 || received != 1 {
		t.Errorf("fallback=%d received=%d, want 0/1", fallback.sent, received)
	}

	// Clearing the override reverts to the fallback once the cache expires.
	lookup.url = ""
	sender.cachedAt = time.Time{} // force refresh
	if _, err := sender.Send(context.Background(), domain.Notification{}); err != nil {
		t.Fatalf("Send after clear: %v", err)
	}
	if fallback.sent != 1 {
		t.Errorf("fallback after clear = %d, want 1", fallback.sent)
	}
}
