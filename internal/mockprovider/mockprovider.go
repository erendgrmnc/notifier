// Package mockprovider is a built-in stand-in for the external delivery
// provider, used by the testing dashboard. The worker delivers to it over
// real HTTP; the dashboard reads back what "clients" received. Content
// markers simulate provider behavior for retry/DLQ demos.
package mockprovider

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	// bufferSize bounds the in-memory inbox; oldest messages drop off.
	bufferSize = 200

	// Content markers driving simulated failures.
	markerRetryable = "FAILME"
	markerPermanent = "REJECTME"
)

// ReceivedMessage is one delivery as the provider saw it.
type ReceivedMessage struct {
	MessageID  string    `json:"message_id"`
	To         string    `json:"to"`
	Channel    string    `json:"channel"`
	Content    string    `json:"content"`
	ReceivedAt time.Time `json:"received_at"`
}

// Store is a mutex-guarded ring buffer of received messages. Memory-only
// by design: it is a testing tool, not delivery history (the
// notifications table is).
type Store struct {
	mu       sync.Mutex
	messages []ReceivedMessage
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) add(message ReceivedMessage) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.messages = append(store.messages, message)
	if len(store.messages) > bufferSize {
		store.messages = store.messages[len(store.messages)-bufferSize:]
	}
}

// Recent returns received messages, newest first.
func (store *Store) Recent() []ReceivedMessage {
	store.mu.Lock()
	defer store.mu.Unlock()

	recent := make([]ReceivedMessage, len(store.messages))
	for i, message := range store.messages {
		recent[len(store.messages)-1-i] = message
	}
	return recent
}

type incomingMessage struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Receive implements the provider wire contract: accepts
// {to, channel, content}, replies 202 {messageId, status, timestamp}.
// FAILME content simulates a retryable outage (500); REJECTME simulates
// a permanent rejection (400).
func (store *Store) Receive(writer http.ResponseWriter, request *http.Request) {
	var incoming incomingMessage
	if err := json.NewDecoder(request.Body).Decode(&incoming); err != nil {
		http.Error(writer, `{"error":"malformed provider request"}`, http.StatusBadRequest)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	switch {
	case strings.Contains(incoming.Content, markerRetryable):
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = writer.Write([]byte(`{"error":"simulated provider outage"}`))
		return
	case strings.Contains(incoming.Content, markerPermanent):
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"error":"simulated permanent rejection"}`))
		return
	}

	received := ReceivedMessage{
		MessageID:  uuid.NewString(),
		To:         incoming.To,
		Channel:    incoming.Channel,
		Content:    incoming.Content,
		ReceivedAt: time.Now().UTC(),
	}
	store.add(received)

	writer.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(writer).Encode(map[string]string{
		"messageId": received.MessageID,
		"status":    "accepted",
		"timestamp": received.ReceivedAt.Format(time.RFC3339),
	})
}
