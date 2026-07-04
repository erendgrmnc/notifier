// Package rabbit implements the AMQP transport: topology declaration,
// publishing, and consuming. It carries messages only — no business rules.
package rabbit

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
)

const (
	// DirectExchange routes notification messages by channel routing key.
	DirectExchange = "notifications.direct"

	// DeadLetterExchange collects exhausted and permanently failed
	// deliveries for inspection; the database remains the source of truth.
	DeadLetterExchange = "notifications.dlx"
	DeadLetterQueue    = "notifications.dlq"

	// EventsExchange fans status-change events out to live listeners
	// (the WebSocket hub). Events are best-effort and transient.
	EventsExchange = "notifications.events"

	// maxPriority enables 0-10 message priorities on the work queues.
	// Queue arguments are immutable after declaration.
	maxPriority = 10
)

// RetryTier is one fixed-delay backoff step. Each tier is a fanout
// exchange feeding a TTL queue whose dead-letter exchange points back at
// DirectExchange. Publishing to the fanout with routing key = channel
// preserves that key through expiry, so the message returns to the
// correct channel work queue. Fixed-TTL tiers avoid the head-of-line
// blocking of per-message TTLs and need no broker plugins.
type RetryTier struct {
	Name string // exchange and queue share this name
	TTL  time.Duration
}

// RetryTiers is ordered by escalating delay. Tier delays are topology
// (baked into immutable queue arguments), not runtime config — changing
// them requires redeclaring the queues.
var RetryTiers = []RetryTier{
	{Name: "notifications.retry.5s", TTL: 5 * time.Second},
	{Name: "notifications.retry.30s", TTL: 30 * time.Second},
	{Name: "notifications.retry.120s", TTL: 120 * time.Second},
}

// TierForAttempt selects the backoff tier after the given 1-based failed
// attempt, clamping to the longest delay when attempts outnumber tiers.
func TierForAttempt(attempt int) RetryTier {
	index := attempt - 1
	if index < 0 {
		index = 0
	}
	if index >= len(RetryTiers) {
		index = len(RetryTiers) - 1
	}
	return RetryTiers[index]
}

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

	for _, tier := range RetryTiers {
		if err := declareRetryTier(channel, tier); err != nil {
			return err
		}
	}

	if err := channel.ExchangeDeclare(EventsExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", EventsExchange, err)
	}

	return declareDeadLetter(channel)
}

// declareRetryTier sets up one backoff step: fanout exchange → TTL queue
// → (on expiry) back to the direct exchange with the original routing key.
func declareRetryTier(channel *amqp.Channel, tier RetryTier) error {
	if err := channel.ExchangeDeclare(tier.Name, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare retry exchange %s: %w", tier.Name, err)
	}

	if _, err := channel.QueueDeclare(tier.Name, true, false, false, false, amqp.Table{
		"x-message-ttl":          tier.TTL.Milliseconds(),
		"x-dead-letter-exchange": DirectExchange,
		// No x-dead-letter-routing-key: the original channel routing key
		// must survive expiry so the message re-enters its work queue.
	}); err != nil {
		return fmt.Errorf("declare retry queue %s: %w", tier.Name, err)
	}

	if err := channel.QueueBind(tier.Name, "", tier.Name, false, nil); err != nil {
		return fmt.Errorf("bind retry queue %s: %w", tier.Name, err)
	}
	return nil
}

func declareDeadLetter(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(DeadLetterExchange, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", DeadLetterExchange, err)
	}
	if _, err := channel.QueueDeclare(DeadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", DeadLetterQueue, err)
	}
	if err := channel.QueueBind(DeadLetterQueue, "", DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind queue %s: %w", DeadLetterQueue, err)
	}
	return nil
}
