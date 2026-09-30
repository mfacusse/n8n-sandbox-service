package api

import "net/http"

type scalerPolicyResponse struct {
	MinNodes            int    `json:"min_nodes"`
	MaxNodes            int    `json:"max_nodes"`
	ScaleOutThreshold   int    `json:"scale_out_threshold"`
	ScaleInThreshold    int    `json:"scale_in_threshold"`
	ScaleInSustainedFor string `json:"scale_in_sustained_for"`
	Cooldown            string `json:"cooldown"`
	EvalInterval        string `json:"eval_interval"`
}

type scalerVMSSResponse struct {
	SubscriptionID string `json:"subscription_id"`
	ResourceGroup  string `json:"resource_group"`
	VMSSName       string `json:"vmss_name"`
}

type scalerDecisionResponse struct {
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

type scalerResponse struct {
	Policy       scalerPolicyResponse    `json:"policy"`
	VMSS         scalerVMSSResponse      `json:"vmss"`
	LastDecision *scalerDecisionResponse `json:"last_decision"`
}

// handleGetScaler serves GET /admin/scaler: the active scaling policy, VMSS
// target, and the most recent evaluation's outcome (FR-007). scaler is nil
// when the capacity scaler is disabled, in which case this returns 503.
func handleGetScaler(scaler *CapacityScaler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !requireAdmin(w, r) {
			return
		}
		if scaler == nil {
			writeError(w, http.StatusServiceUnavailable, "scaler is disabled: SANDBOX_API_SCALER_* is not configured")
			return
		}

		policy := scaler.Policy()
		resp := scalerResponse{
			Policy: scalerPolicyResponse{
				MinNodes:            policy.MinNodes,
				MaxNodes:            policy.MaxNodes,
				ScaleOutThreshold:   policy.ScaleOutThreshold,
				ScaleInThreshold:    policy.ScaleInThreshold,
				ScaleInSustainedFor: policy.ScaleInSustainedFor.String(),
				Cooldown:            policy.Cooldown.String(),
				EvalInterval:        policy.EvalInterval.String(),
			},
			VMSS: scalerVMSSResponse{
				SubscriptionID: policy.AzureSubscriptionID,
				ResourceGroup:  policy.AzureResourceGroup,
				VMSSName:       policy.AzureVMSSName,
			},
		}
		if last := scaler.LastDecision(); last != nil {
			resp.LastDecision = &scalerDecisionResponse{
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
		writeJSON(w, http.StatusOK, resp)
	}
}
