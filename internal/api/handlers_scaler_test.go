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
	"github.com/n8n-io/sandbox-service/internal/scaler"
	"github.com/n8n-io/sandbox-service/internal/scalerclient"
)

// newTestGatewayWithScalerClient builds a gateway router wired to the given
// scaler client (nil is a valid, "disabled" case).
func newTestGatewayWithScalerClient(t *testing.T, adminKey string, client *scalerclient.Client) http.Handler {
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
	}), registry.New(45*time.Second), metrics.NewAPIRecorder(false), client)
	if err != nil {
		t.Fatalf("create gateway router: %v", err)
	}
	return router
}

func TestGetScalerProxiesSuccessResponse(t *testing.T) {
	const backchannelBody = `{"policy":{"min_nodes":2,"max_nodes":10,"scale_out_threshold":5,"scale_in_threshold":30,"scale_in_sustained_for":"10m0s","cooldown":"5m0s","eval_interval":"1m0s"},"vmss":{"subscription_id":"sub-1","resource_group":"rg-1","vmss_name":"vmss-1"},"last_decision":null}`
	backchannel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer good-token" {
			t.Errorf("Authorization header = %q, want Bearer good-token", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(backchannelBody))
	}))
	defer backchannel.Close()

	client := scalerclient.New(backchannel.URL, "good-token", 2*time.Second)
	router := newTestGatewayWithScalerClient(t, "admin-key", client)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/scaler: expected %d, got %d body=%s", http.StatusOK, rr.Code, rr.Body.String())
	}
	var resp scalerclient.PolicyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	wantPolicy := struct {
		MinNodes            int
		MaxNodes            int
		ScaleOutThreshold   int
		ScaleInThreshold    int
		ScaleInSustainedFor string
		Cooldown            string
		EvalInterval        string
	}{2, 10, 5, 30, "10m0s", "5m0s", "1m0s"}
	gotPolicy := struct {
		MinNodes            int
		MaxNodes            int
		ScaleOutThreshold   int
		ScaleInThreshold    int
		ScaleInSustainedFor string
		Cooldown            string
		EvalInterval        string
	}{
		resp.Policy.MinNodes, resp.Policy.MaxNodes, resp.Policy.ScaleOutThreshold, resp.Policy.ScaleInThreshold,
		resp.Policy.ScaleInSustainedFor, resp.Policy.Cooldown, resp.Policy.EvalInterval,
	}
	if gotPolicy != wantPolicy {
		t.Fatalf("Policy = %+v, want %+v", gotPolicy, wantPolicy)
	}
	if resp.VMSS.SubscriptionID != "sub-1" || resp.VMSS.ResourceGroup != "rg-1" || resp.VMSS.VMSSName != "vmss-1" {
		t.Fatalf("VMSS = %+v, want {sub-1 rg-1 vmss-1}", resp.VMSS)
	}
	if resp.LastDecision != nil {
		t.Fatalf("LastDecision = %+v, want nil for the mocked body", resp.LastDecision)
	}
}

func TestGetScalerReturns503WhenBackchannelUnreachable(t *testing.T) {
	backchannel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := backchannel.URL
	backchannel.Close() // nothing listens on addr anymore

	client := scalerclient.New(addr, "token", 2*time.Second)
	router := newTestGatewayWithScalerClient(t, "admin-key", client)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /admin/scaler with unreachable backchannel: expected %d, got %d body=%s", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}
}

func TestGetScalerReturns502WhenBackchannelAuthMisconfigured(t *testing.T) {
	backchannel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer backchannel.Close()

	client := scalerclient.New(backchannel.URL, "wrong-token", 2*time.Second)
	router := newTestGatewayWithScalerClient(t, "admin-key", client)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("GET /admin/scaler with auth-misconfigured backchannel: expected %d, got %d body=%s", http.StatusBadGateway, rr.Code, rr.Body.String())
	}
}

func TestGetScalerReturns503WhenDisabled(t *testing.T) {
	router := newTestGatewayWithScalerClient(t, "admin-key", nil)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /admin/scaler with disabled scaler proxy: expected %d, got %d body=%s", http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	}
}

func TestGetScalerRequiresAdminKey(t *testing.T) {
	router := newTestGatewayWithScalerClient(t, "admin-key", nil)

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

// TestGetScalerEndToEndRealChain exercises the real chain end to end: a real
// internal/scaler.Scaler, its real internal/scaler.NewServer backchannel, a
// real internal/scalerclient.Client, and the real GET /admin/scaler proxy
// handler — proving US3/SC-004's "scaling guarantees and auditability
// survive the split" for the auditability half (the decision-logic half is
// proven in internal/scaler/scaler_test.go).
func TestGetScalerEndToEndRealChain(t *testing.T) {
	source := &scaler.FakeRunnerSource{}
	source.Runners = append(source.Runners, scaler.Runner{
		ID: "r1", Healthy: true, CapacityTotal: 10, CapacityUsed: 8, LastSeen: time.Now(),
	}) // free = 2: will scale-out against the policy below

	policy := scaler.Policy{
		MinNodes: 2, MaxNodes: 10,
		ScaleOutThreshold: 5, ScaleInThreshold: 30,
		ScaleInSustainedFor: 10 * time.Minute, Cooldown: 5 * time.Minute, EvalInterval: time.Minute,
		AzureSubscriptionID: "sub-1", AzureResourceGroup: "rg-1", AzureVMSSName: "vmss-1",
	}
	s := scaler.New(policy, 24*time.Hour, source, azurescale.NewFake(4), metrics.NewScalerRecorder(false))
	s.Evaluate(context.Background(), time.Now())

	backchannel := httptest.NewServer(scaler.NewServer(s, "shared-token"))
	defer backchannel.Close()

	client := scalerclient.New(backchannel.URL, "shared-token", 2*time.Second)
	router := newTestGatewayWithScalerClient(t, "admin-key", client)

	req := httptest.NewRequest(http.MethodGet, "/admin/scaler", nil)
	req.Header.Set("X-Api-Key", "admin-key")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/scaler: expected %d, got %d body=%s", http.StatusOK, rr.Code, rr.Body.String())
	}
	var resp scalerclient.PolicyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Policy.MinNodes != 2 || resp.Policy.MaxNodes != 10 || resp.Policy.ScaleOutThreshold != 5 || resp.Policy.ScaleInThreshold != 30 ||
		resp.Policy.ScaleInSustainedFor != "10m0s" || resp.Policy.Cooldown != "5m0s" || resp.Policy.EvalInterval != "1m0s" {
		t.Fatalf("unexpected policy in response: %+v", resp.Policy)
	}
	if resp.VMSS.SubscriptionID != "sub-1" || resp.VMSS.ResourceGroup != "rg-1" || resp.VMSS.VMSSName != "vmss-1" {
		t.Fatalf("unexpected vmss target in response: %+v", resp.VMSS)
	}
	if resp.LastDecision == nil {
		t.Fatal("expected last_decision to be populated from the real evaluation, got nil")
	}
	d := resp.LastDecision
	if d.Decision != scaler.DecisionScaleOut || d.Reason != scaler.ReasonBelowScaleOutThresh ||
		d.FreeCapacity != 2 || d.TotalCapacity != 10 || !d.SignalAvailable ||
		d.CurrentNodeCount != 4 || d.TargetNodeCount == nil || *d.TargetNodeCount != 5 || d.Error != "" {
		t.Fatalf("last_decision does not match the real scale_out evaluation: %+v", d)
	}
}
