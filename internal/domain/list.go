package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ListQuery filters the notification listing. Zero values mean "no
// filter". Keyset fields resume after the given (created_at, id) pair,
// paging newest-first without offset drift under concurrent inserts.
type ListQuery struct {
	Status  Status
	Channel Channel
	BatchID *uuid.UUID
	From    *time.Time
	To      *time.Time
	Limit   int

	CursorCreatedAt *time.Time
	CursorID        *uuid.UUID
}

// Validate rejects filter values outside the enums. Limits are the
// service layer's concern; time ranges are free-form.
func (query ListQuery) Validate() error {
	var errs ValidationErrors

	if query.Status != "" && !statusValid(query.Status) {
		errs = append(errs, FieldError{Field: "status", Message: fmt.Sprintf("unknown status %q", query.Status)})
	}
	if query.Channel != "" && !query.Channel.Valid() {
		errs = append(errs, FieldError{Field: "channel", Message: fmt.Sprintf("unknown channel %q", query.Channel)})
	}
	if query.From != nil && query.To != nil && query.To.Before(*query.From) {
		errs = append(errs, FieldError{Field: "to", Message: "must not precede from"})
	}

	if len(errs) > 0 {
		return errs
	}
	return nil
}

func statusValid(status Status) bool {
	for _, known := range allStatuses {
		if status == known {
			return true
		}
	}
	return false
}
