package rabbit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
)

// Publisher sends notification messages with publisher confirms, so a
// successful publish means the broker has durably accepted the message.
type Publisher struct {
	mu      sync.Mutex // amqp channels are not safe for concurrent publish
	channel *amqp.Channel
}

// NewPublisher opens a dedicated channel in confirm mode.
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publisher channel: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		channel.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	return &Publisher{channel: channel}, nil
}

// PublishCreated routes a notification message to its channel work queue
// and waits for the broker confirm.
func (publisher *Publisher) PublishCreated(ctx context.Context, notification domain.Notification) error {
	body, err := json.Marshal(Message{NotificationID: notification.ID})
	if err != nil {
		return fmt.Errorf("marshal queue message: %w", err)
	}

	publisher.mu.Lock()
	defer publisher.mu.Unlock()

	confirmation, err := publisher.channel.PublishWithDeferredConfirmWithContext(ctx,
		DirectExchange,
		string(notification.Channel), // routing key
		false,                        // mandatory
		false,                        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish notification %s: %w", notification.ID, err)
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("await publish confirm for %s: %w", notification.ID, err)
	}
	if !acked {
		return fmt.Errorf("broker nacked publish for %s", notification.ID)
	}
	return nil
}

// Close releases the publisher channel.
func (publisher *Publisher) Close() error {
	return publisher.channel.Close()
}
