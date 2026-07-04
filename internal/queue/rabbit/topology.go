// Package rabbit implements the AMQP transport: topology declaration,
// publishing, and consuming. It carries messages only — no business rules.
package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
)

const (
	// DirectExchange routes notification messages by channel routing key.
	DirectExchange = "notifications.direct"

	// maxPriority enables 0-10 message priorities on the work queues.
	// Queue arguments are immutable after declaration.
	maxPriority = 10
)

// WorkQueueName returns the work queue for a delivery channel,
// e.g. notifications.sms.
func WorkQueueName(channel domain.Channel) string {
	return fmt.Sprintf("notifications.%s", channel)
}

// Connect dials RabbitMQ.
func Connect(rabbitURL string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	return conn, nil
}

// DeclareTopology idempotently declares the exchange, the per-channel
// priority work queues, and their bindings. Both api and worker roles
// call this at startup so boot order does not matter.
func DeclareTopology(conn *amqp.Connection) error {
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open declare channel: %w", err)
	}
	defer channel.Close()

	if err := channel.ExchangeDeclare(
		DirectExchange,
		amqp.ExchangeDirect,
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %s: %w", DirectExchange, err)
	}

	for _, deliveryChannel := range domain.Channels() {
		queueName := WorkQueueName(deliveryChannel)

		if _, err := channel.QueueDeclare(
			queueName,
			true,  // durable
			false, // autoDelete
			false, // exclusive
			false, // noWait
			amqp.Table{"x-max-priority": int32(maxPriority)},
		); err != nil {
			return fmt.Errorf("declare queue %s: %w", queueName, err)
		}

		if err := channel.QueueBind(
			queueName,
			string(deliveryChannel), // routing key
			DirectExchange,
			false, // noWait
			nil,
		); err != nil {
			return fmt.Errorf("bind queue %s: %w", queueName, err)
		}
	}

	return nil
}
