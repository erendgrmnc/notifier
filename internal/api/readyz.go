package api

import (
	"context"
	"net/http"
	"time"
)

// readinessCheckTimeout bounds each dependency probe.
const readinessCheckTimeout = 2 * time.Second

// ReadinessChecks maps dependency names to probes. A nil error means
// the dependency can serve traffic.
type ReadinessChecks map[string]func(ctx context.Context) error

// handleReadyz reports per-dependency readiness: 200 when everything is
// reachable, 503 with the failing dependencies named otherwise. Load
// balancers route on the status code; humans read the body.
func handleReadyz(checks ReadinessChecks) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), readinessCheckTimeout)
		defer cancel()

		results := make(map[string]string, len(checks))
		ready := true
		for dependency, check := range checks {
			if err := check(ctx); err != nil {
				results[dependency] = "down: " + err.Error()
				ready = false
			} else {
				results[dependency] = "ok"
			}
		}

		status := http.StatusOK
		overall := "ok"
		if !ready {
			status = http.StatusServiceUnavailable
			overall = "degraded"
		}
		writeJSONResponse(writer, status, map[string]any{"status": overall, "checks": results})
	}
}
