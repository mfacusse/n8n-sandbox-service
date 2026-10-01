package scaler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// testHTTPClient has a short timeout so a stalled handler fails the test
// instead of hanging the suite.
var testHTTPClient = &http.Client{Timeout: 5 * time.Second}

func TestServerGetPolicyWithCorrectTokenReturns200(t *testing.T) {
	source := &FakeRunnerSource{}
	seedRunner(source, "r1", 10, 8) // free = 2: will scale-out
	s, _ := newTestScaler(t, basePolicy, source, 4)
	s.Evaluate(context.Background(), time.Now())

	srv := httptest.NewServer(NewServer(s, "good-token"))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/policy", nil)
	req.Header.Set("Authorization", "Bearer good-token")
	resp, err := testHTTPClient.Do(req)
	if err != nil {
		t.Fatalf("GET /policy: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /policy: expected %d, got %d", http.StatusOK, resp.StatusCode)
	}
	var body policyResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Policy.MinNodes != basePolicy.MinNodes || body.Policy.MaxNodes != basePolicy.MaxNodes {
		t.Fatalf("unexpected policy in response: %+v", body.Policy)
	}
	if body.LastDecision == nil || body.LastDecision.Decision != DecisionScaleOut {
		t.Fatalf("expected last_decision to reflect the seeded scale_out evaluation, got %+v", body.LastDecision)
	}
}

func TestServerGetPolicyMissingOrWrongTokenReturns401(t *testing.T) {
	s := New(basePolicy, testHeartbeatGrace, &FakeRunnerSource{}, azurescale.NewFake(4), metrics.NewScalerRecorder(false))
	srv := httptest.NewServer(NewServer(s, "good-token"))
	defer srv.Close()

	cases := []struct {
		name   string
		header string
	}{
		{"missing", ""},
		{"wrong", "Bearer wrong-token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/policy", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			resp, err := testHTTPClient.Do(req)
			if err != nil {
				t.Fatalf("GET /policy: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("GET /policy (%s token): expected %d, got %d", tc.name, http.StatusUnauthorized, resp.StatusCode)
			}
		})
	}
}

func TestServerGetPolicyNoDecisionYetReturnsNullLastDecision(t *testing.T) {
	s := New(basePolicy, testHeartbeatGrace, &FakeRunnerSource{}, azurescale.NewFake(4), metrics.NewScalerRecorder(false))
	srv := httptest.NewServer(NewServer(s, "good-token"))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/policy", nil)
	req.Header.Set("Authorization", "Bearer good-token")
	resp, err := testHTTPClient.Do(req)
	if err != nil {
		t.Fatalf("GET /policy: %v", err)
	}
	defer resp.Body.Close()

	var body policyResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.LastDecision != nil {
		t.Fatalf("LastDecision = %+v, want nil before any evaluation", body.LastDecision)
	}
}
