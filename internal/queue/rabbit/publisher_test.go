package rabbit

import (
	"testing"

	"notifier/internal/domain"
)

func TestAMQPPriorityMapping(t *testing.T) {
	testCases := []struct {
		priority domain.Priority
		want     uint8
	}{
		{priority: domain.PriorityHigh, want: amqpPriorityHigh},
		{priority: domain.PriorityNormal, want: amqpPriorityNormal},
		{priority: domain.PriorityLow, want: amqpPriorityLow},
		{priority: domain.Priority(""), want: amqpPriorityNormal},
	}

	for _, tc := range testCases {
		if got := amqpPriority(tc.priority); got != tc.want {
			t.Errorf("amqpPriority(%q) = %d, want %d", tc.priority, got, tc.want)
		}
	}
}

func TestTierForAttempt(t *testing.T) {
	testCases := []struct {
		attempt int
		want    string
	}{
		{attempt: 1, want: "notifications.retry.5s"},
		{attempt: 2, want: "notifications.retry.30s"},
		{attempt: 3, want: "notifications.retry.120s"},
		{attempt: 4, want: "notifications.retry.120s"}, // clamped to longest
		{attempt: 0, want: "notifications.retry.5s"},   // defensive floor
	}

	for _, tc := range testCases {
		if got := TierForAttempt(tc.attempt); got.Name != tc.want {
			t.Errorf("TierForAttempt(%d) = %s, want %s", tc.attempt, got.Name, tc.want)
		}
	}
}

func TestRetryTiersEscalate(t *testing.T) {
	for i := 1; i < len(RetryTiers); i++ {
		if RetryTiers[i].TTL <= RetryTiers[i-1].TTL {
			t.Errorf("tier %d TTL %v does not escalate over %v", i, RetryTiers[i].TTL, RetryTiers[i-1].TTL)
		}
	}
}

func TestAMQPPrioritiesOrderCorrectly(t *testing.T) {
	if amqpPriorityHigh <= amqpPriorityNormal || amqpPriorityNormal <= amqpPriorityLow {
		t.Errorf("priority values do not order high > normal > low: %d, %d, %d",
			amqpPriorityHigh, amqpPriorityNormal, amqpPriorityLow)
	}
	if amqpPriorityHigh > maxPriority {
		t.Errorf("high priority %d exceeds queue x-max-priority %d", amqpPriorityHigh, maxPriority)
	}
}
