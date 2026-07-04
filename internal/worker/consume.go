package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
	"notifier/internal/queue/rabbit"
)

// Run consumes every per-channel work queue until the context is
// cancelled. It owns its consumer goroutines: one per channel queue,
// all joined before Run returns.
func (worker *Worker) Run(ctx context.Context, conn *amqp.Connection, prefetch int) error {
	var wg sync.WaitGroup

	for _, deliveryChannel := range domain.Channels() {
		queueName := rabbit.WorkQueueName(deliveryChannel)

		amqpChannel, err := conn.Channel()
		if err != nil {
			return fmt.Errorf("open consumer channel for %s: %w", queueName, err)
		}
		if err := amqpChannel.Qos(prefetch, 0, false); err != nil {
			return fmt.Errorf("set prefetch for %s: %w", queueName, err)
		}

		deliveries, err := amqpChannel.Consume(
			queueName,
			"",    // consumer tag: broker-generated
			false, // autoAck: manual acks only
			false, // exclusive
			false, // noLocal
			false, // noWait
			nil,
		)
		if err != nil {
			return fmt.Errorf("consume %s: %w", queueName, err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer amqpChannel.Close()
			worker.consumeLoop(ctx, queueName, deliveries)
		}()
	}

	worker.logger.Info("worker consuming", slog.Int("queues", len(domain.Channels())), slog.Int("prefetch", prefetch))
	wg.Wait()
	return nil
}

func (worker *Worker) consumeLoop(ctx context.Context, queueName string, deliveries <-chan amqp.Delivery) {
	logger := worker.logger.With(slog.String("queue", queueName))

	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer stopping")
			return
		case delivery, open := <-deliveries:
			if !open {
				logger.Warn("delivery channel closed by broker")
				return
			}
			worker.handleDelivery(ctx, logger, delivery)
		}
	}
}

func (worker *Worker) handleDelivery(ctx context.Context, logger *slog.Logger, delivery amqp.Delivery) {
	var message rabbit.Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		logger.Warn("unparseable message dropped", slog.Any("error", err))
		if err := delivery.Nack(false, false); err != nil {
			logger.Error("nack failed", slog.Any("error", err))
		}
		return
	}

	if err := worker.processNotification(ctx, message.NotificationID); err != nil {
		logger.Error("processing failed; requeueing",
			slog.String("notification_id", message.NotificationID.String()),
			slog.Any("error", err),
		)
		if err := delivery.Nack(false, true); err != nil {
			logger.Error("nack failed", slog.Any("error", err))
		}
		return
	}

	if err := delivery.Ack(false); err != nil {
		logger.Error("ack failed", slog.Any("error", err))
	}
}
