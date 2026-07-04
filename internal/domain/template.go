package domain

import (
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// ErrDuplicateTemplateName is returned when a template name is taken.
var ErrDuplicateTemplateName = errors.New("duplicate template name")

// templateNamePattern keeps names URL-safe: they appear in paths.
var templateNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{1,63}$`)

// Template is a reusable message body with {{.variable}} placeholders,
// rendered into final content at enqueue time. Notifications keep no
// foreign key to it: the stored content is the delivery record, immune
// to later template edits.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ValidateNewTemplate checks shape only; body parseability is the
// renderer's concern and is checked by the service at creation.
func ValidateNewTemplate(template Template) error {
	var errs ValidationErrors

	if !templateNamePattern.MatchString(template.Name) {
		errs = append(errs, FieldError{Field: "name", Message: "must be 2-64 chars of lowercase letters, digits, hyphen or underscore"})
	}
	if !template.Channel.Valid() {
		errs = append(errs, FieldError{Field: "channel", Message: "unknown channel"})
	}
	if template.Body == "" {
		errs = append(errs, FieldError{Field: "body", Message: "required"})
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}
