package domain

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNotFound is returned when a requested notification or template
// does not exist. Repositories translate their driver's no-rows error
// into this sentinel.
var ErrNotFound = errors.New("not found")

// ErrInvalidTransition is returned when a status change is rejected by
// the state machine (in Go) or the guarded UPDATE (in SQL).
var ErrInvalidTransition = errors.New("invalid status transition")

// ErrDuplicateIdempotencyKey is returned when a create collides with an
// existing notification's idempotency key. Callers replay the original.
var ErrDuplicateIdempotencyKey = errors.New("duplicate idempotency key")

// FieldError describes one invalid input field.
type FieldError struct {
	Field   string
	Message string
}

// ValidationErrors aggregates all invalid fields of one request so API
// consumers get everything wrong in a single response.
type ValidationErrors []FieldError

func (v ValidationErrors) Error() string {
	parts := make([]string, len(v))
	for i, fieldErr := range v {
		parts[i] = fmt.Sprintf("%s: %s", fieldErr.Field, fieldErr.Message)
	}
	return "validation failed: " + strings.Join(parts, "; ")
}
