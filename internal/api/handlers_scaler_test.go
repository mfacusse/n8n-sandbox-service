package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/api/store"
	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// newTestGatewayWithScaler builds a gateway router wired to the given
// scaler (nil is a valid, "disabled" case). tenantKey, if non-empty, is
// provisioned via the admin API so tests can exercise the tenant-key path.
func newTestGatewayWithScaler(t *testing.T, adminKey string, scaler *CapacityScaler) http.Handler {
	t.Helper()
	s, err := store.New(":memory:")
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	router, err := NewGatewayRouter(s, withRunnerTLS(&config.APIConfig{
		APIKeys:             map[string]struct{}{adminKey: {}},
		RunnerAPIKey:        "runner-key",
		MaxFileBytes:        1024,
		DefaultMaxSandboxes: 50,
	}), registry.New(45*time.Second), metrics.NewAPIRecorder(false), scaler)
	if err != nil {
		t.Fatalf("create gateway router: %v", err)
	}
	return router
}

func testScalerForHandler(t *testing.T) *CapacityScaler {
	t.Helper()
	reg := registry.NewMemory(45 * time.Second)
	reg.Upsert("r1", "http://127.0.0.1:8080", "127.0.0.1:9091", true, 10, 8, 0)
	cfg := &config.APIConfig{
		HeartbeatGrace: 24 * time.Hour,
		Scaler: &config.ScalerConfig{
			MinNodes:            2,
			MaxNodes:            10,
			ScaleOutThreshold:   5,
			ScaleInThreshold:    30,
			ScaleInSustainedFor: 10 * time.Minute,
			Cooldown:            5 * time.Minute,
			EvalInterval:        time.Minute,
			AzureSubscriptionID: "sub-1",
			AzureResourceGroup:  "rg-1",
			AzureVMSSName:       "vmss-1",
		},
	}
	scaler := NewCapacityScaler(cfg, reg, azurescale.NewFake(4), metrics.NewScalerRecorder(false))
	scaler.Evaluate(context.Background(), time.Now())
	return scaler
}

func TestGetScalerReturnsPolicyAndLastDecision(t *testing.T) {
	scaler := testScalerForHandler(t)
	router := newTestGatewayWithScaler(t, "admin-key", scaler)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/scaler: expected %d, got %d body=%s", http.StatusOK, rr.Code, rr.Body.String())
	}
	var resp scalerResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Policy.MinNodes != 2 || resp.Policy.MaxNodes != 10 {
		t.Fatalf("unexpected policy in response: %+v", resp.Policy)
	}
	if resp.VMSS.ResourceGroup != "rg-1" || resp.VMSS.VMSSName != "vmss-1" {
		t.Fatalf("unexpected vmss target in response: %+v", resp.VMSS)
	}
	if resp.LastDecision == nil || resp.LastDecision.Decision != DecisionScaleOut {
		t.Fatalf("expected last_decision to reflect the seeded scale_out evaluation, got %+v", resp.LastDecision)
	}
}

func TestGetScalerRequiresAdminKey(t *testing.T) {
	scaler := testScalerForHandler(t)
	router := newTestGatewayWithScaler(t, "admin-key", scaler)

	// No key at all: AuthMiddleware rejects with 401 before the handler runs.
	noKeyReq := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	noKeyRR := httptest.NewRecorder()
	router.ServeHTTP(noKeyRR, noKeyReq)
	if noKeyRR.Code != http.StatusUnauthorized {
		t.Fatalf("no API key: expected %d, got %d", http.StatusUnauthorized, noKeyRR.Code)
	}

	// An unrecognized key: also 401 from AuthMiddleware.
	bogusReq := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	bogusReq.Header.Set("X-Api-Key", "not-a-real-key")
	bogusRR := httptest.NewRecorder()
	router.ServeHTTP(bogusRR, bogusReq)
	if bogusRR.Code != http.StatusUnauthorized {
		t.Fatalf("invalid API key: expected %d, got %d", http.StatusUnauthorized, bogusRR.Code)
	}

	// A real tenant key: authenticated, but not admin -> 403 from requireAdmin.
	createReq := httptest.NewRequest(http.MethodPost, "/admin/tenants", strings.NewReader(`{"name":"acme"}`))
	createReq.Header.Set("X-Api-Key", "admin-key")
	createReq.Header.Set("Content-Type", "application/json")
	createRR := httptest.NewRecorder()
	router.ServeHTTP(createRR, createReq)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("setup: create tenant: expected %d, got %d body=%s", http.StatusCreated, createRR.Code, createRR.Body.String())
	}
	var created createTenantResponse
	if err := json.Unmarshal(createRR.Body.Bytes(), &created); err != nil {
		t.Fatalf("setup: decode create tenant response: %v", err)
	}

	tenantReq := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	tenantReq.Header.Set("X-Api-Key", created.Key.APIKey)
	tenantRR := httptest.NewRecorder()
	router.ServeHTTP(tenantRR, tenantReq)
	if tenantRR.Code != http.StatusForbidden {
		t.Fatalf("tenant key: expected %d, got %d body=%s", http.StatusForbidden, tenantRR.Code, tenantRR.Body.String())
	}
}

func TestGetScalerReturns503WhenDisabled(t *testing.T) {
	router := newTestGatewayWithScaler(t, "admin-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /admin/scaler with disabled scaler: expected %d, got %d body=%s", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}
}
