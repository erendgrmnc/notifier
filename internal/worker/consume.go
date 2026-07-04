package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
	"notifier/internal/queue/rabbit"
)

const (
	// pausePollInterval is how often consumers check the pause flag.
	pausePollInterval = time.Second
	// pauseCheckTimeout bounds the DB read behind each check.
	pauseCheckTimeout = 2 * time.Second
)

// Run consumes every per-channel work queue until the context is
// cancelled or a consumer dies. Each queue gets a supervision goroutine
// that honours the pause flag: while paused the AMQP subscription is
// cancelled entirely, so messages accumulate as ready in the queue. A
// broker-closed delivery channel remains fatal — the process must exit
// (and be restarted) rather than keep running with dead consumers.
func (worker *Worker) Run(ctx context.Context, conn *amqp.Connection, prefetch int) error {
	runCtx, stopAll := context.WithCancel(ctx)
	defer stopAll()

	var wg sync.WaitGroup
	loopErrs := make(chan error, len(domain.Channels()))

	for _, deliveryChannel := range domain.Channels() {
		queueName := rabbit.WorkQueueName(deliveryChannel)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := worker.superviseQueue(runCtx, conn, queueName, prefetch); err != nil {
				loopErrs <- err
				stopAll() // one dead consumer stops the siblings so Run can report
			}
		}()
	}

	worker.logger.Info("worker consuming", slog.Int("queues", len(domain.Channels())), slog.Int("prefetch", prefetch))
	wg.Wait()
	close(loopErrs)

	for err := range loopErrs {
		if err != nil {
			return err
		}
	}
	return nil
}

// superviseQueue alternates between waiting out pauses and consuming.
func (worker *Worker) superviseQueue(ctx context.Context, conn *amqp.Connection, queueName string, prefetch int) error {
	logger := worker.logger.With(slog.String("queue", queueName))

	for ctx.Err() == nil {
		worker.waitWhilePaused(ctx, logger)
		if ctx.Err() != nil {
			return nil
		}

		if err := worker.consumeUntilPaused(ctx, conn, queueName, prefetch, logger); err != nil {
			return err
		}
	}
	return nil
}

// waitWhilePaused blocks while the pause flag is set.
func (worker *Worker) waitWhilePaused(ctx context.Context, logger *slog.Logger) {
	announced := false
	for worker.isPaused(ctx) {
		if !announced {
			logger.Info("consumer paused")
			announced = true
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(pausePollInterval):
		}
	}
	if announced {
		logger.Info("consumer resuming")
	}
}

// consumeUntilPaused subscribes and processes deliveries until the pause
// flag is set (returns nil after cancelling the subscription), the
// context ends (nil), or the broker closes the channel (error).
func (worker *Worker) consumeUntilPaused(ctx context.Context, conn *amqp.Connection, queueName string, prefetch int, logger *slog.Logger) error {
	amqpChannel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open consumer channel for %s: %w", queueName, err)
	}
	defer amqpChannel.Close()

	if err := amqpChannel.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("set prefetch for %s: %w", queueName, err)
	}

	consumerTag := queueName + ".consumer"
	deliveries, err := amqpChannel.Consume(
		queueName,
		consumerTag,
		false, // autoAck: manual acks only
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queueName, err)
	}

	pauseTicker := time.NewTicker(pausePollInterval)
	defer pauseTicker.Stop()
	cancelRequested := false

	for {
		select {
		case <-ctx.Done():
			logger.Info("consumer stopping")
			return nil
		case <-pauseTicker.C:
			if !cancelRequested && worker.isPaused(ctx) {
				cancelRequested = true
				// Cancel returns prefetched-but-unacked messages to the
				// queue; the delivery channel closes once drained.
				if err := amqpChannel.Cancel(consumerTag, false); err != nil {
					return fmt.Errorf("cancel consumer for %s: %w", queueName, err)
				}
			}
		case delivery, open := <-deliveries:
			if !open {
				if cancelRequested {
					return nil // expected closure: paused
				}
				return fmt.Errorf("delivery channel for %s closed by broker", queueName)
			}
			worker.handleDelivery(ctx, logger, delivery)
		}
	}
}

// isPaused checks the shared pause flag, keeping the last known state
// when the check itself fails.
func (worker *Worker) isPaused(ctx context.Context) bool {
	checkCtx, cancel := context.WithTimeout(ctx, pauseCheckTimeout)
	defer cancel()

	paused, err := worker.pause.WorkerPaused(checkCtx)
	if err != nil {
		if ctx.Err() == nil {
			worker.logger.Warn("pause check failed; keeping last state", slog.Any("error", err))
		}
		return worker.lastPaused.Load()
	}
	worker.lastPaused.Store(paused)
	return paused
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
