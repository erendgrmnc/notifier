package delivery

// Result is a successful delivery's outcome: the provider's message ID
// plus a snapshot of the raw response for the dashboard and diagnostics.
type Result struct {
	ProviderMessageID string
	StatusCode        int
	// Body is the provider's response body, truncated to a display-safe
	// length. Ephemeral: it rides status events, never the database.
	Body string
}
