package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"notifier/internal/domain"
)

// SendError is a classified provider failure. Retryable failures go to
// the backoff tiers; permanent ones go straight to failed + DLQ.
type SendError struct {
	StatusCode int // 0 for network-level failures
	Retryable  bool
	Message    string
}

func (sendErr *SendError) Error() string {
	if sendErr.StatusCode != 0 {
		return fmt.Sprintf("provider returned %d: %s", sendErr.StatusCode, sendErr.Message)
	}
	return sendErr.Message
}

// IsRetryable classifies any delivery error. Unknown error types default
// to retryable — the status guards make a duplicate attempt harmless,
// while wrongly marking permanent loses the notification.
func IsRetryable(err error) bool {
	var sendErr *SendError
	if errors.As(err, &sendErr) {
		return sendErr.Retryable
	}
	return true
}

// providerRequest is the wire format the assessment specifies.
type providerRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

type providerResponse struct {
	MessageID string `json:"messageId"`
}

// WebhookSender delivers notifications to the external HTTP provider
// (webhook.site in the assessment setup).
type WebhookSender struct {
	client      *http.Client
	providerURL string
}

func NewWebhookSender(providerURL string, timeout time.Duration) *WebhookSender {
	return &WebhookSender{
		client:      &http.Client{Timeout: timeout},
		providerURL: providerURL,
	}
}

func (sender *WebhookSender) Send(ctx context.Context, notification domain.Notification) (string, error) {
	payload, err := json.Marshal(providerRequest{
		To:      notification.Recipient,
		Channel: string(notification.Channel),
		Content: notification.Content,
	})
	if err != nil {
		return "", fmt.Errorf("marshal provider request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, sender.providerURL, bytes.NewReader(payload))
	if err != nil {
		return "", fmt.Errorf("build provider request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := sender.client.Do(request)
	if err != nil {
		return "", &SendError{Retryable: true, Message: fmt.Sprintf("provider unreachable: %v", err)}
	}
	defer response.Body.Close()

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		return parseProviderMessageID(response.Body), nil
	case response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500:
		return "", &SendError{StatusCode: response.StatusCode, Retryable: true, Message: readErrorBody(response.Body)}
	default:
		return "", &SendError{StatusCode: response.StatusCode, Retryable: false, Message: readErrorBody(response.Body)}
	}
}

// parseProviderMessageID tolerates providers (like a fresh webhook.site
// URL) that return 2xx without the documented JSON body.
func parseProviderMessageID(body io.Reader) string {
	var parsed providerResponse
	if err := json.NewDecoder(body).Decode(&parsed); err != nil {
		return ""
	}
	return parsed.MessageID
}

func readErrorBody(body io.Reader) string {
	const maxErrorBytes = 512
	raw, err := io.ReadAll(io.LimitReader(body, maxErrorBytes))
	if err != nil || len(raw) == 0 {
		return "no response body"
	}
	return string(raw)
}
