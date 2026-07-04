package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

func startHub(t *testing.T) (*Hub, *httptest.Server, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	hub := NewHub(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	go hub.Run(ctx)

	server := httptest.NewServer(NewRouter(RouterConfig{
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RequestTimeout: time.Second,
		EventHub:       hub,
	}))
	t.Cleanup(server.Close)
	return hub, server, ctx
}

func dialWS(t *testing.T, ctx context.Context, url string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func eventJSON(t *testing.T, id uuid.UUID, status string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"notification_id": id, "status": status})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func readEvent(t *testing.T, ctx context.Context, conn *websocket.Conn) map[string]any {
	t.Helper()
	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, payload, err := conn.Read(readCtx)
	if err != nil {
		t.Fatalf("read event: %v", err)
	}
	var event map[string]any
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	return event
}

func TestHubBroadcastReachesClient(t *testing.T) {
	hub, server, ctx := startHub(t)
	conn := dialWS(t, ctx, "ws"+server.URL[4:]+"/ws")

	// Registration races the broadcast; retry until delivered.
	id := uuid.New()
	deadline := time.Now().Add(2 * time.Second)
	go func() {
		for time.Now().Before(deadline) {
			hub.BroadcastRaw(eventJSON(t, id, "sent"))
			time.Sleep(50 * time.Millisecond)
		}
	}()

	event := readEvent(t, ctx, conn)
	if event["status"] != "sent" || event["notification_id"] != id.String() {
		t.Errorf("event = %v, want sent for %s", event, id)
	}
}

func TestHubFilterByNotificationID(t *testing.T) {
	hub, server, ctx := startHub(t)
	wanted := uuid.New()
	other := uuid.New()
	conn := dialWS(t, ctx, "ws"+server.URL[4:]+"/ws?id="+wanted.String())

	deadline := time.Now().Add(2 * time.Second)
	go func() {
		for time.Now().Before(deadline) {
			hub.BroadcastRaw(eventJSON(t, other, "queued"))
			hub.BroadcastRaw(eventJSON(t, wanted, "sent"))
			time.Sleep(50 * time.Millisecond)
		}
	}()

	event := readEvent(t, ctx, conn)
	if event["notification_id"] != wanted.String() {
		t.Errorf("filtered stream leaked %v", event)
	}
}

func TestWSRejectsMalformedFilter(t *testing.T) {
	_, server, ctx := startHub(t)

	conn, _, err := websocket.Dial(ctx, "ws"+server.URL[4:]+"/ws?id=not-a-uuid", nil)
	if err == nil {
		// Server accepts the upgrade then closes with a policy violation.
		_, _, readErr := conn.Read(ctx)
		conn.CloseNow()
		if readErr == nil {
			t.Fatal("connection with malformed filter stayed open")
		}
	}
}
