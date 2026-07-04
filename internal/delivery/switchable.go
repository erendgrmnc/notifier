package delivery

import (
	"context"
	"sync"
	"time"

	"notifier/internal/domain"
)

// overrideCacheTTL bounds how stale the cached override may be — a
// dashboard provider switch takes effect within this window.
const overrideCacheTTL = 2 * time.Second

// Sender matches the worker's consumer-side interface so senders can
// wrap each other.
type Sender interface {
	Send(ctx context.Context, notification domain.Notification) (Result, error)
}

// OverrideLookup reads the shared runtime provider override.
type OverrideLookup interface {
	ProviderOverride(ctx context.Context) (string, error)
}

// SwitchableSender delivers to the runtime-overridden provider URL when
// one is set (the dashboard's webhook.site integration), falling back to
// the statically configured sender otherwise. The override is cached
// briefly so the delivery hot path does not query per message.
type SwitchableSender struct {
	lookup   OverrideLookup
	fallback Sender
	timeout  time.Duration

	mu        sync.Mutex
	cachedURL string
	cachedAt  time.Time
	webhook   *WebhookSender // rebuilt when the override URL changes
}

func NewSwitchableSender(lookup OverrideLookup, fallback Sender, timeout time.Duration) *SwitchableSender {
	return &SwitchableSender{lookup: lookup, fallback: fallback, timeout: timeout}
}

func (sender *SwitchableSender) Send(ctx context.Context, notification domain.Notification) (Result, error) {
	target := sender.currentTarget(ctx)
	if target == nil {
		return sender.fallback.Send(ctx, notification)
	}
	return target.Send(ctx, notification)
}

// currentTarget returns the webhook sender for the active override, or
// nil when no override is set. A failed lookup keeps the last state.
func (sender *SwitchableSender) currentTarget(ctx context.Context) *WebhookSender {
	sender.mu.Lock()
	defer sender.mu.Unlock()

	if time.Since(sender.cachedAt) > overrideCacheTTL {
		if overrideURL, err := sender.lookup.ProviderOverride(ctx); err == nil {
			if overrideURL != sender.cachedURL {
				sender.cachedURL = overrideURL
				sender.webhook = nil
				if overrideURL != "" {
					sender.webhook = NewWebhookSender(overrideURL, sender.timeout)
				}
			}
			sender.cachedAt = time.Now()
		}
	}
	return sender.webhook
}
