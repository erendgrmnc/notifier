package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

const (
	// clientSendBuffer bounds per-client backlog; a client that cannot
	// keep up is dropped rather than allowed to stall the hub.
	clientSendBuffer = 32
	// clientWriteTimeout bounds each frame write.
	clientWriteTimeout = 5 * time.Second
)

// hubEvent is one status event plus the ID clients may filter on.
type hubEvent struct {
	notificationID uuid.UUID
	payload        []byte
}

// wsClient is one connected listener.
type wsClient struct {
	send chan []byte
	// filterID limits the stream to one notification; uuid.Nil means all.
	filterID uuid.UUID
}

// Hub owns every WebSocket client from a single goroutine: register,
// unregister, and broadcast are channel operations, so no client map
// locking exists anywhere.
type Hub struct {
	register   chan *wsClient
	unregister chan *wsClient
	broadcast  chan hubEvent
	// done closes when Run exits so handlers never block sending to a
	// hub that no longer receives (shutdown races the hijacked conns,
	// which http.Server.Shutdown does not drain).
	done   chan struct{}
	logger *slog.Logger
}

func NewHub(logger *slog.Logger) *Hub {
	return &Hub{
		register:   make(chan *wsClient),
		unregister: make(chan *wsClient),
		broadcast:  make(chan hubEvent, 64),
		done:       make(chan struct{}),
		logger:     logger,
	}
}

// Run owns the client set until ctx ends. Only this goroutine touches
// the map; only it closes client send channels.
func (hub *Hub) Run(ctx context.Context) {
	clients := make(map[*wsClient]struct{})
	defer close(hub.done)

	for {
		select {
		case <-ctx.Done():
			for client := range clients {
				close(client.send)
			}
			hub.logger.Info("websocket hub stopped")
			return
		case client := <-hub.register:
			clients[client] = struct{}{}
		case client := <-hub.unregister:
			if _, known := clients[client]; known {
				delete(clients, client)
				close(client.send)
			}
		case event := <-hub.broadcast:
			for client := range clients {
				if client.filterID != uuid.Nil && client.filterID != event.notificationID {
					continue
				}
				select {
				case client.send <- event.payload:
				default:
					// Slow client: drop it instead of blocking the hub.
					delete(clients, client)
					close(client.send)
				}
			}
		}
	}
}

// Broadcast fans one event to connected clients. Non-blocking: if the
// hub's buffer is full the event is dropped (events are best-effort UX).
func (hub *Hub) Broadcast(notificationID uuid.UUID, payload []byte) {
	select {
	case hub.broadcast <- hubEvent{notificationID: notificationID, payload: payload}:
	default:
	}
}

// serveWS upgrades the connection and streams events until the client
// disconnects or the hub closes it.
func (hub *Hub) serveWS(writer http.ResponseWriter, request *http.Request) {
	conn, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return // Accept already replied with an error status
	}

	client := &wsClient{send: make(chan []byte, clientSendBuffer)}
	if idParam := request.URL.Query().Get("id"); idParam != "" {
		filterID, err := uuid.Parse(idParam)
		if err != nil {
			conn.Close(websocket.StatusPolicyViolation, "id must be a UUID")
			return
		}
		client.filterID = filterID
	}

	select {
	case hub.register <- client:
	case <-hub.done:
		conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	defer func() {
		select {
		case hub.unregister <- client:
		case <-hub.done:
		}
	}()

	// Reader: discard inbound frames, detect disconnect.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, _, err := conn.Read(request.Context()); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-readerDone:
			conn.Close(websocket.StatusNormalClosure, "")
			return
		case payload, open := <-client.send:
			if !open {
				conn.Close(websocket.StatusGoingAway, "server shutting down")
				return
			}
			writeCtx, cancel := context.WithTimeout(request.Context(), clientWriteTimeout)
			err := conn.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				conn.Close(websocket.StatusAbnormalClosure, "write failed")
				return
			}
		}
	}
}

// eventPayload re-marshals is unnecessary: events arrive as JSON from the
// queue; BroadcastRaw extracts the notification id for filtering.
type eventEnvelope struct {
	NotificationID uuid.UUID `json:"notification_id"`
}

// BroadcastRaw parses the notification ID out of a raw event payload and
// fans it out. Unparseable payloads are dropped.
func (hub *Hub) BroadcastRaw(payload []byte) {
	var envelope eventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		hub.logger.Warn("unparseable status event dropped", slog.Any("error", err))
		return
	}
	hub.Broadcast(envelope.NotificationID, payload)
}
