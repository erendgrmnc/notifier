package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// listCursor is the keyset pagination token: the (created_at, id) pair
// of the last row a page returned, opaque to clients as base64 JSON.
type listCursor struct {
	CreatedAt time.Time `json:"t"`
	ID        uuid.UUID `json:"id"`
}

func encodeCursor(createdAt time.Time, id uuid.UUID) string {
	raw, _ := json.Marshal(listCursor{CreatedAt: createdAt, ID: id})
	return base64.URLEncoding.EncodeToString(raw)
}

func decodeCursor(encoded string) (listCursor, error) {
	raw, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return listCursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	var cursor listCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return listCursor{}, fmt.Errorf("parse cursor: %w", err)
	}
	if cursor.CreatedAt.IsZero() || cursor.ID == uuid.Nil {
		return listCursor{}, fmt.Errorf("cursor missing fields")
	}
	return cursor, nil
}
