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

func TestAMQPPrioritiesOrderCorrectly(t *testing.T) {
	if !(amqpPriorityHigh > amqpPriorityNormal && amqpPriorityNormal > amqpPriorityLow) {
		t.Errorf("priority values do not order high > normal > low: %d, %d, %d",
			amqpPriorityHigh, amqpPriorityNormal, amqpPriorityLow)
	}
	if amqpPriorityHigh > maxPriority {
		t.Errorf("high priority %d exceeds queue x-max-priority %d", amqpPriorityHigh, maxPriority)
	}
}
