package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/api/store"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

func newTestGatewayWithScalerURL(t *testing.T, adminKey, scalerURL string) http.Handler {
	t.Helper()
	s, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	router, err := NewGatewayRouter(s, withRunnerTLS(&config.APIConfig{
		APIKeys:      map[string]struct{}{adminKey: {}},
		RunnerAPIKey: "runner-key",
		MaxFileBytes: 1024,
		ScalerURL:    scalerURL,
	}), registry.New(45*time.Second), metrics.NewAPIRecorder(false))
	if err != nil {
		t.Fatalf("create gateway router: %v", err)
	}
	return router
}

func TestGetScalerReturns503WhenNotConfigured(t *testing.T) {
	router := newTestGatewayWithScalerURL(t, "admin-key", "")

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d body=%s", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}
}

func TestGetScalerRelaysSuccessResponse(t *testing.T) {
	const body = `{"policy":{"min_nodes":2,"max_nodes":10},"vmss":{"vmss_name":"vmss-1"},"last_decision":null}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/policy" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer backend.Close()

	router := newTestGatewayWithScalerURL(t, "admin-key", backend.URL)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected %d, got %d body=%s", http.StatusOK, rr.Code, rr.Body.String())
	}
	if rr.Body.String() != body {
		t.Fatalf("response body = %s, want %s (unchanged relay)", rr.Body.String(), body)
	}
}

func TestGetScalerReturns503WhenUnreachable(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := backend.URL
	backend.Close() // nothing listens on addr anymore

	router := newTestGatewayWithScalerURL(t, "admin-key", addr)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected %d, got %d body=%s", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}
}

func TestGetScalerRequiresAdminKey(t *testing.T) {
	router := newTestGatewayWithScalerURL(t, "admin-key", "")

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no api key: expected %d, got %d", http.StatusUnauthorized, rr.Code)
	}
}
