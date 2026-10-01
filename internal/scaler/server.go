package scaler

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
)

// Wire shapes for GET /policy — see
// specs/002-scaler-standalone-service/contracts/scaler-internal-api.md.
// Deliberately independent of internal/scalerclient's matching types: each
// side of this internal contract is written independently against the same
// documented shape, consistent with this package's zero-dependency-on-
// internal/api (and, by the same reasoning, zero-dependency-on-the-API's-
// own-client-package) stance.
type policyResponse struct {
	Policy       policyPolicyFields    `json:"policy"`
	VMSS         policyVMSSFields      `json:"vmss"`
	LastDecision *policyDecisionFields `json:"last_decision"`
}

type policyPolicyFields struct {
	MinNodes            int    `json:"min_nodes"`
	MaxNodes            int    `json:"max_nodes"`
	ScaleOutThreshold   int    `json:"scale_out_threshold"`
	ScaleInThreshold    int    `json:"scale_in_threshold"`
	ScaleInSustainedFor string `json:"scale_in_sustained_for"`
	Cooldown            string `json:"cooldown"`
	EvalInterval        string `json:"eval_interval"`
}

type policyVMSSFields struct {
	SubscriptionID string `json:"subscription_id"`
	ResourceGroup  string `json:"resource_group"`
	VMSSName       string `json:"vmss_name"`
}

type policyDecisionFields struct {
	ObservedAt       int64  `json:"observed_at"`
	FreeCapacity     int    `json:"free_capacity"`
	TotalCapacity    int    `json:"total_capacity"`
	SignalAvailable  bool   `json:"signal_available"`
	CurrentNodeCount int    `json:"current_node_count"`
	Decision         string `json:"decision"`
	Reason           string `json:"reason"`
	TargetNodeCount  *int   `json:"target_node_count,omitempty"`
	Error            string `json:"error,omitempty"`
}

// NewServer returns the internal HTTP handler serving GET /policy,
// authenticated with a shared bearer token. Intended to be served on an
// in-cluster-only listener (SANDBOX_SCALER_LISTEN_ADDR) — see
// contracts/scaler-internal-api.md's Non-goals for why a single shared
// secret is sufficient here instead of mTLS/per-caller identity.
func NewServer(s *Scaler, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /policy", handleGetPolicy(s))
	return requireBearerToken(token, mux)
}

func requireBearerToken(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func handleGetPolicy(s *Scaler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		policy := s.Policy()
		resp := policyResponse{
			Policy: policyPolicyFields{
				MinNodes:            policy.MinNodes,
				MaxNodes:            policy.MaxNodes,
				ScaleOutThreshold:   policy.ScaleOutThreshold,
				ScaleInThreshold:    policy.ScaleInThreshold,
				ScaleInSustainedFor: policy.ScaleInSustainedFor.String(),
				Cooldown:            policy.Cooldown.String(),
				EvalInterval:        policy.EvalInterval.String(),
			},
			VMSS: policyVMSSFields{
				SubscriptionID: policy.AzureSubscriptionID,
				ResourceGroup:  policy.AzureResourceGroup,
				VMSSName:       policy.AzureVMSSName,
			},
		}
		if last := s.LastDecision(); last != nil {
			resp.LastDecision = &policyDecisionFields{
				ObservedAt:       last.ObservedAt.Unix(),
				FreeCapacity:     last.FreeCapacity,
				TotalCapacity:    last.TotalCapacity,
				SignalAvailable:  last.SignalAvailable,
				CurrentNodeCount: last.CurrentNodeCount,
				Decision:         last.Decision,
				Reason:           last.Reason,
				TargetNodeCount:  last.TargetNodeCount,
				Error:            last.Error,
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
