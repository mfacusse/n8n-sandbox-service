// Package scalerclient is the tenant-facing API's client for the standalone
// scaler's internal backchannel (contracts/scaler-internal-api.md in
// specs/002-scaler-standalone-service). It exists so GET /admin/scaler can
// proxy to the standalone component without the API ever depending on the
// scaler's internals.
package scalerclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ErrUnreachable means the backchannel call failed at the connection/HTTP
// level (refused, timed out, non-200/401 status) — the scaler is down or
// not deployed.
var ErrUnreachable = errors.New("scaler unreachable")

// ErrUnauthorized means the backchannel rejected the configured token — a
// deployment misconfiguration (API and scaler disagree on the shared
// secret), distinct from the scaler simply being down.
var ErrUnauthorized = errors.New("scaler auth misconfigured")

// PolicyResponse is the GET /policy response shape (same field shapes as
// GET /admin/scaler's policy/vmss/last_decision — see
// contracts/scaler-internal-api.md).
type PolicyResponse struct {
	Policy struct {
		MinNodes            int    `json:"min_nodes"`
		MaxNodes            int    `json:"max_nodes"`
		ScaleOutThreshold   int    `json:"scale_out_threshold"`
		ScaleInThreshold    int    `json:"scale_in_threshold"`
		ScaleInSustainedFor string `json:"scale_in_sustained_for"`
		Cooldown            string `json:"cooldown"`
		EvalInterval        string `json:"eval_interval"`
	} `json:"policy"`
	VMSS struct {
		SubscriptionID string `json:"subscription_id"`
		ResourceGroup  string `json:"resource_group"`
		VMSSName       string `json:"vmss_name"`
	} `json:"vmss"`
	LastDecision *struct {
		ObservedAt       int64  `json:"observed_at"`
		FreeCapacity     int    `json:"free_capacity"`
		TotalCapacity    int    `json:"total_capacity"`
		SignalAvailable  bool   `json:"signal_available"`
		CurrentNodeCount int    `json:"current_node_count"`
		Decision         string `json:"decision"`
		Reason           string `json:"reason"`
		TargetNodeCount  *int   `json:"target_node_count,omitempty"`
		Error            string `json:"error,omitempty"`
	} `json:"last_decision"`
}

// Client calls the standalone scaler's internal backchannel.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// New builds a Client. timeout bounds the entire request (connect + read) so
// a dead or slow scaler never hangs the caller (FR-007/FR-012).
func New(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		token:      token,
		httpClient: &http.Client{Timeout: timeout},
	}
}

// GetPolicy fetches the active policy and most recent decision. Returns
// ErrUnreachable for any connection/transport-level failure or unexpected
// status, and ErrUnauthorized specifically for a 401 (bad/missing token).
func (c *Client) GetPolicy(ctx context.Context) (*PolicyResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/policy", nil)
	if err != nil {
		return nil, fmt.Errorf("scalerclient: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, ErrUnauthorized
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: unexpected status %d", ErrUnreachable, resp.StatusCode)
	}

	var out PolicyResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("scalerclient: decode response: %w", err)
	}
	return &out, nil
}
