package rabbit

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConsumeEvents binds an exclusive auto-delete queue to the events
// fanout and hands every payload to handle. It returns when ctx ends or
// the broker closes the stream — events are best-effort, so a broker
// hiccup degrades the live stream rather than killing the process.
func ConsumeEvents(ctx context.Context, conn *amqp.Connection, logger *slog.Logger, handle func(payload []byte)) error {
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open events channel: %w", err)
	}
	defer channel.Close()

	// Exclusive + auto-delete: each api instance gets its own copy of
	// every event, and the queue vanishes with the connection.
	queue, err := channel.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return fmt.Errorf("declare events queue: %w", err)
	}
	if err := channel.QueueBind(queue.Name, "", EventsExchange, false, nil); err != nil {
		return fmt.Errorf("bind events queue: %w", err)
	}

	deliveries, err := channel.Consume(queue.Name, "", true /* autoAck: fire-and-forget */, true, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume events: %w", err)
	}

	logger.Info("event stream consuming", slog.String("queue", queue.Name))
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, open := <-deliveries:
			if !open {
				logger.Warn("event stream closed by broker; live updates degraded")
				return nil
			}
			handle(delivery.Body)
		}
	}
}
