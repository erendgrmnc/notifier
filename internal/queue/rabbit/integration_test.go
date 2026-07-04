package rabbit

// Integration tests against a real RabbitMQ. Skipped unless
// TEST_AMQP_URL is set (CI provides a service container; locally the
// test scripts provision a dedicated notifier_test vhost so the live
// worker cannot consume test messages):
//
//	TEST_AMQP_URL="amqp://notifier:notifier@localhost:5672/notifier_test" go test ./internal/queue/rabbit/
//
// These cover what unit tests cannot: topology declaration, channel
// routing with priorities, the retry tier's TTL dead-lettering back to
// the original work queue, the DLQ, the events fanout, and confirms.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"notifier/internal/domain"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

var (
	integrationConn     *amqp.Connection
	integrationConnOnce sync.Once
)

func integrationBroker(t *testing.T) (*amqp.Connection, *Publisher) {
	t.Helper()
	amqpURL := os.Getenv("TEST_AMQP_URL")
	if amqpURL == "" {
		t.Skip("TEST_AMQP_URL not set; skipping rabbitmq integration tests")
	}

	integrationConnOnce.Do(func() {
		conn, err := Connect(amqpURL)
		if err != nil {
			t.Fatalf("connect test broker: %v", err)
		}
		// Twice on purpose: declaration must be idempotent.
		for i := 0; i < 2; i++ {
			if err := DeclareTopology(conn); err != nil {
				t.Fatalf("declare topology (pass %d): %v", i+1, err)
			}
		}
		integrationConn = conn
	})

	purgeAllQueues(t, integrationConn)
	publisher, err := NewPublisher(integrationConn)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	t.Cleanup(func() { publisher.Close() })
	return integrationConn, publisher
}

func purgeAllQueues(t *testing.T, conn *amqp.Connection) {
	t.Helper()
	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("open purge channel: %v", err)
	}
	defer channel.Close()

	names := []string{DeadLetterQueue}
	for _, deliveryChannel := range domain.Channels() {
		names = append(names, WorkQueueName(deliveryChannel))
	}
	for _, tier := range RetryTiers {
		names = append(names, tier.Name)
	}
	for _, name := range names {
		if _, err := channel.QueuePurge(name, false); err != nil {
			t.Fatalf("purge %s: %v", name, err)
		}
	}
}

// consumeOne waits for a single delivery on the queue.
func consumeOne(t *testing.T, conn *amqp.Connection, queueName string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("open consume channel: %v", err)
	}
	t.Cleanup(func() { channel.Close() })

	deliveries, err := channel.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume %s: %v", queueName, err)
	}
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(timeout):
		t.Fatalf("no delivery on %s within %s", queueName, timeout)
		return amqp.Delivery{}
	}
}

func testQueueNotification(channel domain.Channel, priority domain.Priority) domain.Notification {
	return domain.Notification{ID: uuid.New(), Channel: channel, Priority: priority}
}

func TestIntegrationPublishCreatedRoutesToChannelQueue(t *testing.T) {
	conn, publisher := integrationBroker(t)

	notification := testQueueNotification(domain.ChannelSMS, domain.PriorityHigh)
	if err := publisher.PublishCreated(context.Background(), notification); err != nil {
		t.Fatalf("PublishCreated: %v", err)
	}

	delivery := consumeOne(t, conn, WorkQueueName(domain.ChannelSMS), 3*time.Second)
	var message Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	if message.NotificationID != notification.ID {
		t.Errorf("routed id = %s, want %s", message.NotificationID, notification.ID)
	}
	if delivery.Priority != 9 {
		t.Errorf("amqp priority = %d, want 9 for high", delivery.Priority)
	}
}

func TestIntegrationRetryTierReturnsToWorkQueue(t *testing.T) {
	conn, publisher := integrationBroker(t)

	// Attempt 1 maps to the 5s tier; TTL expiry must dead-letter the
	// message back to its original work queue with the routing key
	// (= channel) preserved.
	notification := testQueueNotification(domain.ChannelEmail, domain.PriorityNormal)
	published := time.Now()
	if err := publisher.PublishRetry(context.Background(), notification, 1); err != nil {
		t.Fatalf("PublishRetry: %v", err)
	}

	delivery := consumeOne(t, conn, WorkQueueName(domain.ChannelEmail), 10*time.Second)
	elapsed := time.Since(published)
	if elapsed < 4*time.Second {
		t.Errorf("retry arrived after %s, want >= ~5s TTL", elapsed)
	}
	var message Message
	if err := json.Unmarshal(delivery.Body, &message); err != nil || message.NotificationID != notification.ID {
		t.Errorf("retry payload = %s (err %v), want original id", delivery.Body, err)
	}
}

func TestIntegrationDeadLetterQueue(t *testing.T) {
	conn, publisher := integrationBroker(t)

	notification := testQueueNotification(domain.ChannelPush, domain.PriorityLow)
	if err := publisher.PublishDeadLetter(context.Background(), notification, "attempts exhausted"); err != nil {
		t.Fatalf("PublishDeadLetter: %v", err)
	}

	delivery := consumeOne(t, conn, DeadLetterQueue, 3*time.Second)
	var deadLetter DeadLetterMessage
	if err := json.Unmarshal(delivery.Body, &deadLetter); err != nil {
		t.Fatalf("unmarshal dead letter: %v", err)
	}
	if deadLetter.NotificationID != notification.ID || deadLetter.Reason != "attempts exhausted" {
		t.Errorf("dead letter = %+v, want id and reason preserved", deadLetter)
	}
}

func TestIntegrationEventsFanOutToConsumers(t *testing.T) {
	conn, publisher := integrationBroker(t)

	received := make(chan []byte, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = ConsumeEvents(ctx, conn, discardLogger(), func(payload []byte) {
			select {
			case received <- payload:
			default:
			}
		})
	}()
	// Give the exclusive queue a moment to bind before publishing.
	time.Sleep(300 * time.Millisecond)

	event := StatusEvent{NotificationID: uuid.New(), Status: "sent", Channel: "sms", OccurredAt: time.Now()}
	if err := publisher.PublishEvent(context.Background(), event); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	select {
	case payload := <-received:
		var decoded StatusEvent
		if err := json.Unmarshal(payload, &decoded); err != nil || decoded.NotificationID != event.NotificationID {
			t.Errorf("fanned-out payload = %s (err %v)", payload, err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no event fanned out within 3s")
	}

	cancel()
	select {
	case <-consumerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ConsumeEvents did not stop on cancel")
	}
}

func TestIntegrationBulkPublishConfirmsAndInspection(t *testing.T) {
	conn, publisher := integrationBroker(t)

	batch := make([]domain.Notification, 25)
	for i := range batch {
		batch[i] = testQueueNotification(domain.ChannelSMS, domain.PriorityNormal)
	}
	confirmed, err := publisher.PublishCreatedAll(context.Background(), batch)
	if err != nil {
		t.Fatalf("PublishCreatedAll: %v", err)
	}
	if len(confirmed) != len(batch) {
		t.Fatalf("confirmed %d of %d publishes", len(confirmed), len(batch))
	}

	depths, err := NewInspector(conn).QueueDepths(context.Background())
	if err != nil {
		t.Fatalf("QueueDepths: %v", err)
	}
	for _, depth := range depths {
		if depth.Name == WorkQueueName(domain.ChannelSMS) && depth.Ready != len(batch) {
			t.Errorf("sms queue depth = %d, want %d", depth.Ready, len(batch))
		}
	}
}
