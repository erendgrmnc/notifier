package rabbit

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
)

// QueueDepth is one queue's ready-message count.
type QueueDepth struct {
	Name  string `json:"name"`
	Ready int    `json:"ready"`
}

// Inspector reads queue depths for dashboards and metrics.
type Inspector struct {
	conn *amqp.Connection
}

func NewInspector(conn *amqp.Connection) *Inspector {
	return &Inspector{conn: conn}
}

// QueueDepths reports ready counts for every service queue (work queues,
// retry tiers, DLQ) via passive declares — cheaper than the management
// API and needs no extra credentials. The context bounds the probes;
// amqp091's channel API has no per-call ctx, so cancellation is checked
// between queue probes.
func (inspector *Inspector) QueueDepths(ctx context.Context) ([]QueueDepth, error) {
	channel, err := inspector.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open inspect channel: %w", err)
	}
	defer channel.Close()

	var names []string
	for _, deliveryChannel := range domain.Channels() {
		names = append(names, WorkQueueName(deliveryChannel))
	}
	for _, tier := range RetryTiers {
		names = append(names, tier.Name)
	}
	names = append(names, DeadLetterQueue)

	depths := make([]QueueDepth, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("inspect queues: %w", err)
		}
		queue, err := channel.QueueDeclarePassive(name, true, false, false, false, nil)
		if err != nil {
			return nil, fmt.Errorf("inspect queue %s: %w", name, err)
		}
		depths = append(depths, QueueDepth{Name: name, Ready: queue.Messages})
	}
	return depths, nil
}
