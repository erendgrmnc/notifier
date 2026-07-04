package domain

import (
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

// Per-channel content limits.
const (
	smsMaxRunes   = 160
	pushMaxBytes  = 512
	emailMaxBytes = 100 * 1024
)

var (
	// e164Pattern matches international phone numbers: +, then 7-15 digits.
	e164Pattern = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)
	// emailPattern is a deliberate shape check, not RFC 5322 — the
	// provider is the final authority on deliverability.
	emailPattern = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)
)

// ValidateNew checks a notification about to be created. It returns
// ValidationErrors listing every failing field, or nil.
func ValidateNew(notification Notification, now time.Time) error {
	var errs ValidationErrors

	if !notification.Channel.Valid() {
		errs = append(errs, FieldError{Field: "channel", Message: fmt.Sprintf("unknown channel %q", notification.Channel)})
	}
	if !notification.Priority.Valid() {
		errs = append(errs, FieldError{Field: "priority", Message: fmt.Sprintf("unknown priority %q", notification.Priority)})
	}

	errs = append(errs, validateRecipient(notification.Channel, notification.Recipient)...)
	errs = append(errs, validateContent(notification.Channel, notification.Content)...)

	if notification.ScheduledAt != nil && !notification.ScheduledAt.After(now) {
		errs = append(errs, FieldError{Field: "scheduled_at", Message: "must be in the future"})
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func validateRecipient(channel Channel, recipient string) ValidationErrors {
	if recipient == "" {
		return ValidationErrors{{Field: "recipient", Message: "required"}}
	}

	switch channel {
	case ChannelSMS:
		if !e164Pattern.MatchString(recipient) {
			return ValidationErrors{{Field: "recipient", Message: "must be an E.164 phone number, e.g. +905551234567"}}
		}
	case ChannelEmail:
		if !emailPattern.MatchString(recipient) {
			return ValidationErrors{{Field: "recipient", Message: "must be an email address"}}
		}
	case ChannelPush:
		// Device token formats are provider-specific; presence is enough.
	}
	return nil
}

func validateContent(channel Channel, content string) ValidationErrors {
	if content == "" {
		return ValidationErrors{{Field: "content", Message: "required"}}
	}

	switch channel {
	case ChannelSMS:
		if utf8.RuneCountInString(content) > smsMaxRunes {
			return ValidationErrors{{Field: "content", Message: fmt.Sprintf("sms content exceeds %d characters", smsMaxRunes)}}
		}
	case ChannelPush:
		if len(content) > pushMaxBytes {
			return ValidationErrors{{Field: "content", Message: fmt.Sprintf("push content exceeds %d bytes", pushMaxBytes)}}
		}
	case ChannelEmail:
		if len(content) > emailMaxBytes {
			return ValidationErrors{{Field: "content", Message: fmt.Sprintf("email content exceeds %d bytes", emailMaxBytes)}}
		}
	}
	return nil
}
