package domain

import "testing"

func TestChannelValid(t *testing.T) {
	for _, channel := range []Channel{ChannelSMS, ChannelEmail, ChannelPush} {
		if !channel.Valid() {
			t.Errorf("Channel %q reported invalid", channel)
		}
	}
	if Channel("fax").Valid() {
		t.Error("Channel fax reported valid")
	}
}

func TestPriorityValid(t *testing.T) {
	for _, priority := range []Priority{PriorityHigh, PriorityNormal, PriorityLow} {
		if !priority.Valid() {
			t.Errorf("Priority %q reported invalid", priority)
		}
	}
	if Priority("urgent").Valid() {
		t.Error("Priority urgent reported valid")
	}
}

func TestCanTransition(t *testing.T) {
	testCases := []struct {
		name    string
		from    Status
		to      Status
		allowed bool
	}{
		{name: "pending to queued", from: StatusPending, to: StatusQueued, allowed: true},
		{name: "pending to processing for publish race", from: StatusPending, to: StatusProcessing, allowed: true},
		{name: "scheduled to queued", from: StatusScheduled, to: StatusQueued, allowed: true},
		{name: "queued to processing", from: StatusQueued, to: StatusProcessing, allowed: true},
		{name: "processing to sent", from: StatusProcessing, to: StatusSent, allowed: true},
		{name: "processing to retrying", from: StatusProcessing, to: StatusRetrying, allowed: true},
		{name: "processing to failed", from: StatusProcessing, to: StatusFailed, allowed: true},
		{name: "retrying to processing", from: StatusRetrying, to: StatusProcessing, allowed: true},
		{name: "retrying to failed", from: StatusRetrying, to: StatusFailed, allowed: true},
		{name: "pending cancellable", from: StatusPending, to: StatusCancelled, allowed: true},
		{name: "scheduled cancellable", from: StatusScheduled, to: StatusCancelled, allowed: true},
		{name: "queued cancellable", from: StatusQueued, to: StatusCancelled, allowed: true},
		{name: "processing not cancellable", from: StatusProcessing, to: StatusCancelled, allowed: false},
		{name: "retrying not cancellable", from: StatusRetrying, to: StatusCancelled, allowed: false},
		{name: "sent is terminal", from: StatusSent, to: StatusQueued, allowed: false},
		{name: "failed is terminal", from: StatusFailed, to: StatusQueued, allowed: false},
		{name: "cancelled is terminal", from: StatusCancelled, to: StatusQueued, allowed: false},
		{name: "pending cannot skip to sent", from: StatusPending, to: StatusSent, allowed: false},
		{name: "queued cannot skip to sent", from: StatusQueued, to: StatusSent, allowed: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanTransition(tc.from, tc.to); got != tc.allowed {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", tc.from, tc.to, got, tc.allowed)
			}
		})
	}
}

func TestCancellableStatuses(t *testing.T) {
	want := []Status{StatusPending, StatusScheduled, StatusQueued}
	got := CancellableStatuses()

	if len(got) != len(want) {
		t.Fatalf("CancellableStatuses() = %v, want %v", got, want)
	}
	for i, status := range want {
		if got[i] != status {
			t.Errorf("CancellableStatuses()[%d] = %s, want %s", i, got[i], status)
		}
	}
}
