package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validSMS() Notification {
	return Notification{
		Recipient: "+905551234567",
		Channel:   ChannelSMS,
		Content:   "hello",
		Priority:  PriorityNormal,
	}
}

func TestValidateNewAcceptsValidNotifications(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Hour)

	testCases := []struct {
		name   string
		mutate func(*Notification)
	}{
		{name: "valid sms", mutate: func(*Notification) {}},
		{name: "valid email", mutate: func(n *Notification) {
			n.Channel = ChannelEmail
			n.Recipient = "user@example.com"
		}},
		{name: "valid push", mutate: func(n *Notification) {
			n.Channel = ChannelPush
			n.Recipient = "device-token-f00"
		}},
		{name: "valid scheduled", mutate: func(n *Notification) {
			n.ScheduledAt = &future
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			notification := validSMS()
			tc.mutate(&notification)
			if err := ValidateNew(notification, now); err != nil {
				t.Errorf("ValidateNew = %v, want nil", err)
			}
		})
	}
}

func TestValidateNewRejectsInvalidNotifications(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)

	testCases := []struct {
		name      string
		mutate    func(*Notification)
		wantField string
	}{
		{name: "unknown channel", mutate: func(n *Notification) {
			n.Channel = Channel("fax")
		}, wantField: "channel"},
		{name: "unknown priority", mutate: func(n *Notification) {
			n.Priority = Priority("urgent")
		}, wantField: "priority"},
		{name: "empty recipient", mutate: func(n *Notification) {
			n.Recipient = ""
		}, wantField: "recipient"},
		{name: "sms recipient not E.164", mutate: func(n *Notification) {
			n.Recipient = "0555 123 45 67"
		}, wantField: "recipient"},
		{name: "email recipient malformed", mutate: func(n *Notification) {
			n.Channel = ChannelEmail
			n.Recipient = "not-an-email"
		}, wantField: "recipient"},
		{name: "empty content", mutate: func(n *Notification) {
			n.Content = ""
		}, wantField: "content"},
		{name: "sms content over 160 chars", mutate: func(n *Notification) {
			n.Content = strings.Repeat("x", smsMaxRunes+1)
		}, wantField: "content"},
		{name: "push content over 512 bytes", mutate: func(n *Notification) {
			n.Channel = ChannelPush
			n.Recipient = "device-token-f00"
			n.Content = strings.Repeat("x", pushMaxBytes+1)
		}, wantField: "content"},
		{name: "email content over 100KB", mutate: func(n *Notification) {
			n.Channel = ChannelEmail
			n.Recipient = "user@example.com"
			n.Content = strings.Repeat("x", emailMaxBytes+1)
		}, wantField: "content"},
		{name: "scheduled_at in the past", mutate: func(n *Notification) {
			n.ScheduledAt = &past
		}, wantField: "scheduled_at"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			notification := validSMS()
			tc.mutate(&notification)

			err := ValidateNew(notification, now)
			if err == nil {
				t.Fatal("ValidateNew = nil, want validation error")
			}

			var validationErrs ValidationErrors
			if !errors.As(err, &validationErrs) {
				t.Fatalf("error type = %T, want ValidationErrors", err)
			}
			for _, fieldErr := range validationErrs {
				if fieldErr.Field == tc.wantField {
					return
				}
			}
			t.Errorf("no error for field %q in %v", tc.wantField, validationErrs)
		})
	}
}

func TestSMSContentLimitCountsRunes(t *testing.T) {
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)

	notification := validSMS()
	notification.Content = strings.Repeat("ğ", smsMaxRunes) // multi-byte but exactly 160 runes

	if err := ValidateNew(notification, now); err != nil {
		t.Errorf("ValidateNew rejected %d-rune multibyte sms: %v", smsMaxRunes, err)
	}
}
