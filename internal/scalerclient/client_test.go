package scalerclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGetPolicyUnreachableReturnsBoundedTimeErrorNotHang(t *testing.T) {
	const serverDelay = 200 * time.Millisecond
	const clientTimeout = 50 * time.Millisecond

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(serverDelay) // far longer than the client's timeout below
	}))
	defer srv.Close()

	c := New(srv.URL, "token", clientTimeout)

	start := time.Now()
	_, err := c.GetPolicy(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("GetPolicy() error = nil, want a timeout-triggered error")
	}
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("GetPolicy() error = %v, want wrapping ErrUnreachable", err)
	}
	if elapsed > serverDelay {
		t.Fatalf("GetPolicy() took %v, want well under the %v server delay (bounded by the %v client timeout)", elapsed, serverDelay, clientTimeout)
	}
}

func TestGetPolicyConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close() // nothing listens on addr anymore

	c := New(addr, "token", 2*time.Second)
	_, err := c.GetPolicy(context.Background())
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("GetPolicy() error = %v, want wrapping ErrUnreachable", err)
	}
}

func TestGetPolicyUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL, "wrong-token", 2*time.Second)
	_, err := c.GetPolicy(context.Background())
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("GetPolicy() error = %v, want ErrUnauthorized", err)
	}
}

func TestGetPolicySuccess(t *testing.T) {
	const want = `{"policy":{"min_nodes":2,"max_nodes":10,"scale_out_threshold":5,"scale_in_threshold":30,"scale_in_sustained_for":"10m0s","cooldown":"5m0s","eval_interval":"1m0s"},"vmss":{"subscription_id":"sub-1","resource_group":"rg-1","vmss_name":"vmss-1"},"last_decision":null}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer good-token" {
			t.Errorf("Authorization header = %q, want %q", got, "Bearer good-token")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(want))
	}))
	defer srv.Close()

	c := New(srv.URL, "good-token", 2*time.Second)
	resp, err := c.GetPolicy(context.Background())
	if err != nil {
		t.Fatalf("GetPolicy() error = %v, want nil", err)
	}
	if resp.Policy.MinNodes != 2 || resp.VMSS.VMSSName != "vmss-1" {
		t.Fatalf("GetPolicy() = %+v, decoded incorrectly", resp)
	}
	if resp.LastDecision != nil {
		t.Fatalf("LastDecision = %+v, want nil", resp.LastDecision)
	}
}
