package mockprovider

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func postMessage(t *testing.T, store *Store, content string) *httptest.ResponseRecorder {
	t.Helper()
	body := fmt.Sprintf(`{"to":"+905551234567","channel":"sms","content":%q}`, content)
	recorder := httptest.NewRecorder()
	store.Receive(recorder, httptest.NewRequest(http.MethodPost, "/provider/messages", strings.NewReader(body)))
	return recorder
}

func TestReceiveStoresAndAccepts(t *testing.T) {
	store := NewStore()

	recorder := postMessage(t, store, "hello client")

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	var response map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response["messageId"] == "" || response["status"] != "accepted" || response["timestamp"] == "" {
		t.Errorf("response %v missing contract fields", response)
	}

	recent := store.Recent()
	if len(recent) != 1 || recent[0].Content != "hello client" {
		t.Errorf("Recent() = %+v, want the stored message", recent)
	}
}

func TestReceiveFailureMarkers(t *testing.T) {
	store := NewStore()

	if code := postMessage(t, store, "please FAILME now").Code; code != http.StatusInternalServerError {
		t.Errorf("FAILME status = %d, want 500", code)
	}
	if code := postMessage(t, store, "REJECTME entirely").Code; code != http.StatusBadRequest {
		t.Errorf("REJECTME status = %d, want 400", code)
	}
	if len(store.Recent()) != 0 {
		t.Error("failed deliveries were stored as received")
	}
}

func TestRecentIsNewestFirstAndBounded(t *testing.T) {
	store := NewStore()
	for i := 0; i < bufferSize+10; i++ {
		postMessage(t, store, fmt.Sprintf("message %d", i))
	}

	recent := store.Recent()
	if len(recent) != bufferSize {
		t.Fatalf("buffer holds %d, want %d", len(recent), bufferSize)
	}
	if recent[0].Content != fmt.Sprintf("message %d", bufferSize+9) {
		t.Errorf("newest first violated: recent[0] = %q", recent[0].Content)
	}
}
