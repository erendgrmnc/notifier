package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
	"notifier/internal/observability"
)

// AMQP message priorities within the queues' x-max-priority=10 range.
const (
	amqpPriorityHigh   uint8 = 9
	amqpPriorityNormal uint8 = 5
	amqpPriorityLow    uint8 = 1
)

// amqpPriority maps the domain priority onto the broker's numeric scale.
func amqpPriority(priority domain.Priority) uint8 {
	switch priority {
	case domain.PriorityHigh:
		return amqpPriorityHigh
	case domain.PriorityLow:
		return amqpPriorityLow
	default:
		return amqpPriorityNormal
	}
}

// Publisher sends notification messages with publisher confirms, so a
// successful publish means the broker has durably accepted the message.
// Status events travel a separate fire-and-forget channel: they are
// transient UX, and paying a confirm round-trip per event would throttle
// the delivery hot path that emits them.
type Publisher struct {
	mu      sync.Mutex // amqp channels are not safe for concurrent publish
	channel *amqp.Channel

	eventMu      sync.Mutex
	eventChannel *amqp.Channel
}

// NewPublisher opens the confirmed state channel and the unconfirmed
// events channel.
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	eventChannel, err := conn.Channel()
	if err != nil {
		channel.Close()
		return nil, fmt.Errorf("open event channel: %w", err)
	}
	return &Publisher{channel: channel, eventChannel: eventChannel}, nil
}

// PublishCreated routes a notification message to its channel work queue
// and waits for the broker confirm.
func (publisher *Publisher) PublishCreated(ctx context.Context, notification domain.Notification) error {
	return publisher.publishNotification(ctx, DirectExchange, notification)
}

// PublishRetry places a notification on the backoff tier matching the
// failed attempt; the tier's TTL routes it back to its work queue.
func (publisher *Publisher) PublishRetry(ctx context.Context, notification domain.Notification, attempt int) error {
	return publisher.publishNotification(ctx, TierForAttempt(attempt).Name, notification)
}

// PublishDeadLetter records an exhausted or permanently failed delivery
// on the DLQ for inspection.
func (publisher *Publisher) PublishDeadLetter(ctx context.Context, notification domain.Notification, reason string) error {
	body, err := json.Marshal(DeadLetterMessage{
		NotificationID: notification.ID,
		Reason:         reason,
	})
	if err != nil {
		return fmt.Errorf("marshal dead-letter message: %w", err)
	}
	return publisher.publishConfirmed(ctx, DeadLetterExchange, "", amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

// PublishEvent fans a status change out to live listeners. Best-effort
// and unconfirmed: callers log failures and move on — events are UX,
// not state, and must not slow the delivery path that emits them.
func (publisher *Publisher) PublishEvent(ctx context.Context, event StatusEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal status event: %w", err)
	}

	publisher.eventMu.Lock()
	defer publisher.eventMu.Unlock()
	if err := publisher.eventChannel.PublishWithContext(ctx, EventsExchange, "", false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body, // transient: no persistence for ephemeral events
	}); err != nil {
		return fmt.Errorf("publish status event: %w", err)
	}
	return nil
}

// PublishCreatedAll publishes a burst of notifications under one lock
// acquisition, then awaits all confirms together — the total wait is one
// broker round-trip, not one per message. Returns the IDs the broker
// durably accepted; unconfirmed rows stay pending for sweeper recovery.
func (publisher *Publisher) PublishCreatedAll(ctx context.Context, notifications []domain.Notification) ([]uuid.UUID, error) {
	if len(notifications) == 0 {
		return nil, nil
	}

	type inFlight struct {
		id           uuid.UUID
		confirmation *amqp.DeferredConfirmation
	}
	correlationID := observability.CorrelationIDFrom(ctx)

	publisher.mu.Lock()
	pending := make([]inFlight, 0, len(notifications))
	for _, notification := range notifications {
		body, err := json.Marshal(Message{NotificationID: notification.ID})
		if err != nil {
			publisher.mu.Unlock()
			return nil, fmt.Errorf("marshal queue message: %w", err)
		}
		confirmation, err := publisher.channel.PublishWithDeferredConfirmWithContext(ctx,
			DirectExchange, string(notification.Channel), false, false, amqp.Publishing{
				ContentType:   "application/json",
				DeliveryMode:  amqp.Persistent,
				Priority:      amqpPriority(notification.Priority),
				CorrelationId: correlationID,
				Body:          body,
			})
		if err != nil {
			// Stop the burst; already-published messages still confirm
			// below, the rest stay pending.
			break
		}
		pending = append(pending, inFlight{id: notification.ID, confirmation: confirmation})
	}
	publisher.mu.Unlock()

	confirmed := make([]uuid.UUID, 0, len(pending))
	for _, flight := range pending {
		if acked, err := flight.confirmation.WaitContext(ctx); err == nil && acked {
			confirmed = append(confirmed, flight.id)
		}
	}
	return confirmed, nil
}

// publishNotification sends the standard queue message for a notification
// to the given exchange, keyed by channel so the routing key survives
// retry-tier expiry.
func (publisher *Publisher) publishNotification(ctx context.Context, exchange string, notification domain.Notification) error {
	body, err := json.Marshal(Message{NotificationID: notification.ID})
	if err != nil {
		return fmt.Errorf("marshal queue message: %w", err)
	}
	return publisher.publishConfirmed(ctx, exchange, string(notification.Channel), amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Priority:     amqpPriority(notification.Priority),
		Body:         body,
	})
}

// publishConfirmed publishes one message and waits for the broker confirm.
// The context's correlation ID rides the AMQP header so worker logs join
// the API request logs.
func (publisher *Publisher) publishConfirmed(ctx context.Context, exchange, routingKey string, publishing amqp.Publishing) error {
	publishing.CorrelationId = observability.CorrelationIDFrom(ctx)

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	confirmation, err := publisher.channel.PublishWithDeferredConfirmWithContext(ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", exchange, err)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("await publish confirm from %s: %w", exchange, err)
	}
	if !acked {
		return fmt.Errorf("broker nacked publish to %s", exchange)
	}
	return nil
}

// Close releases both publisher channels.
func (publisher *Publisher) Close() error {
	eventErr := publisher.eventChannel.Close()
	if err := publisher.channel.Close(); err != nil {
		return err
	}
	return eventErr
}
