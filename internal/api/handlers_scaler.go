package api

import (
	"errors"
	"net/http"

	"github.com/n8n-io/sandbox-service/internal/scalerclient"
)

// handleGetScaler serves GET /admin/scaler: the active scaling policy, VMSS
// target, and the most recent evaluation's outcome, proxied from the
// standalone scaler component's internal backchannel
// (contracts/scaler-internal-api.md in specs/002-scaler-standalone-service).
// client is nil when the scaler proxy is not configured (SANDBOX_API_SCALER_URL
// unset), in which case this returns 503.
func handleGetScaler(client *scalerclient.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		if client == nil {
			writeError(w, http.StatusServiceUnavailable, "scaler is disabled: SANDBOX_API_SCALER_URL is not configured")
			return
		}

		resp, err := client.GetPolicy(r.Context())
		if err != nil {
			switch {
			case errors.Is(err, scalerclient.ErrUnauthorized):
				writeError(w, http.StatusBadGateway, "scaler auth misconfigured")
			default:
				writeError(w, http.StatusServiceUnavailable, "scaler unreachable")
			}
			return
		}
		writeJSON(w, http.StatusOK, resp)
	}
}
