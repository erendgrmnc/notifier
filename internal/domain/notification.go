// Package domain holds the pure notification model: types, state machine,
// validation, and sentinel errors. It must not import I/O packages.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// Channel is the delivery medium. Values match the Postgres channel enum.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel.
func Channels() []Channel {
	return []Channel{ChannelSMS, ChannelEmail, ChannelPush}
}

func (c Channel) Valid() bool {
	switch c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return true
	}
	return false
}

// Priority orders processing. Values match the Postgres priority enum.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

func (p Priority) Valid() bool {
	switch p {
	case PriorityHigh, PriorityNormal, PriorityLow:
		return true
	}
	return false
}

// Status is the notification lifecycle state. Values match the Postgres
// notification_status enum.
type Status string

const (
	StatusPending    Status = "pending"
	StatusScheduled  Status = "scheduled"
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// transitions is the single source of truth for the status state machine.
// The SQL layer enforces the same rules via guarded UPDATEs.
var transitions = map[Status][]Status{
	StatusPending:    {StatusQueued, StatusCancelled},
	StatusScheduled:  {StatusQueued, StatusCancelled},
	StatusQueued:     {StatusProcessing, StatusCancelled},
	StatusProcessing: {StatusSent, StatusRetrying, StatusFailed},
	StatusRetrying:   {StatusProcessing, StatusFailed},
	StatusSent:       nil,
	StatusFailed:     nil,
	StatusCancelled:  nil,
}

// CanTransition reports whether moving from one status to another is legal.
func CanTransition(from, to Status) bool {
	for _, allowed := range transitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

// CancellableStatuses lists the states a cancel request may act on.
func CancellableStatuses() []Status {
	return []Status{StatusPending, StatusScheduled, StatusQueued}
}

// Notification is one message to one recipient over one channel.
type Notification struct {
	ID                uuid.UUID
	BatchID           *uuid.UUID
	Recipient         string
	Channel           Channel
	Content           string
	Priority          Priority
	Status            Status
	IdempotencyKey    *string
	ScheduledAt       *time.Time
	Attempts          int
	LastError         *string
	ProviderMessageID *string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	SentAt            *time.Time
}
