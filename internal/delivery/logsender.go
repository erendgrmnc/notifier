// Package delivery implements provider senders behind the worker's
// Sender interface.
package delivery

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"notifier/internal/domain"
)

// LogSender simulates a provider by logging the send and returning a
// generated message ID. It stands in until the webhook provider lands.
type LogSender struct {
	logger *slog.Logger
}

func NewLogSender(logger *slog.Logger) *LogSender {
	return &LogSender{logger: logger}
}

func (sender *LogSender) Send(_ context.Context, notification domain.Notification) (Result, error) {
	providerMessageID := uuid.NewString()
	sender.logger.Info("simulated delivery",
		slog.String("notification_id", notification.ID.String()),
		slog.String("channel", string(notification.Channel)),
		slog.String("recipient", notification.Recipient),
		slog.String("provider_message_id", providerMessageID),
	)
	return Result{
		ProviderMessageID: providerMessageID,
		StatusCode:        202,
		Body:              fmt.Sprintf(`{"messageId":%q,"status":"accepted","simulated":true}`, providerMessageID),
	}, nil
}
