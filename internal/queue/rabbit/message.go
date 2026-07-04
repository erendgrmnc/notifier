package rabbit

import (
	"time"

	"github.com/google/uuid"
)

// Message is the queue payload. It carries the notification identity
// only — workers load current state from the database, so a stale
// payload can never override a cancel or a completed send.
type Message struct {
	NotificationID uuid.UUID `json:"notification_id"`
}

// DeadLetterMessage is the DLQ payload; the database row remains the
// source of truth, this exists for queue-side inspection.
type DeadLetterMessage struct {
	NotificationID uuid.UUID `json:"notification_id"`
	Reason         string    `json:"reason"`
}

// StatusEvent is the live status-change payload fanned out to WebSocket
// listeners. Transient by design: the notifications table is history.
type StatusEvent struct {
	NotificationID uuid.UUID `json:"notification_id"`
	Status         string    `json:"status"`
	Channel        string    `json:"channel"`
	Attempts       int       `json:"attempts"`
	LastError      string    `json:"last_error,omitempty"`
	// Provider fields are set on sent events so live listeners can show
	// exactly what the provider answered.
	ProviderMessageID string    `json:"provider_message_id,omitempty"`
	ProviderResponse  string    `json:"provider_response,omitempty"`
	OccurredAt        time.Time `json:"occurred_at"`
}
