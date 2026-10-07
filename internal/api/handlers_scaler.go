package api

import (
	"io"
	"net/http"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
)

// scalerForwardTimeout bounds the forwarded GET .../policy call. Fixed, not
// configurable — specs/005-simple-scaler-proxy deliberately has exactly one
// scaler-related setting (ScalerURL).
const scalerForwardTimeout = 5 * time.Second

// handleGetScaler forwards to cfg.ScalerURL + "/policy" and relays the
// response unchanged. See specs/005-simple-scaler-proxy/contracts/admin-scaler-api.md.
func handleGetScaler(cfg *config.APIConfig) http.HandlerFunc {
	client := &http.Client{Timeout: scalerForwardTimeout}
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		if cfg.ScalerURL == "" {
			writeError(w, http.StatusServiceUnavailable, "scaler not configured")
			return
		}

		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, cfg.ScalerURL+"/policy", nil)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "scaler unavailable")
			return
		}
		resp, err := client.Do(req)
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "scaler unavailable")
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			writeError(w, http.StatusServiceUnavailable, "scaler unavailable")
			return
		}

		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, resp.Body)
	}
}
