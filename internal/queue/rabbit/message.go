package rabbit

import "github.com/google/uuid"

// Message is the queue payload. It carries the notification identity
// only — workers load current state from the database, so a stale
// payload can never override a cancel or a completed send.
type Message struct {
	NotificationID uuid.UUID `json:"notification_id"`
}
