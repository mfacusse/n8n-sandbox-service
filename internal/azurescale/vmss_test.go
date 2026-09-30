package azurescale

import (
	"context"
	"errors"
	"testing"
)

// Compile-time check: Fake satisfies VMSSScaler, the same contract the
// armcompute-backed implementation satisfies.
var _ VMSSScaler = (*Fake)(nil)

func TestFakeSetCapacityCalledExactlyOnceWithTarget(t *testing.T) {
	f := NewFake(4)
	ctx := context.Background()

	if err := f.SetCapacity(ctx, 7); err != nil {
		t.Fatalf("SetCapacity() error = %v, want nil", err)
	}

	if got := f.CallCount(); got != 1 {
		t.Fatalf("CallCount() = %d, want 1", got)
	}
	target, ok := f.LastTarget()
	if !ok || target != 7 {
		t.Fatalf("LastTarget() = (%d, %v), want (7, true)", target, ok)
	}

	got, err := f.CurrentCapacity(ctx)
	if err != nil {
		t.Fatalf("CurrentCapacity() error = %v, want nil", err)
	}
	if got != 7 {
		t.Fatalf("CurrentCapacity() = %d, want 7 (reflects the SetCapacity call)", got)
	}
}

func TestFakeSetCapacityPropagatesError(t *testing.T) {
	f := NewFake(4)
	f.SetErr = errors.New("azure unavailable")

	if err := f.SetCapacity(context.Background(), 7); err == nil {
		t.Fatal("SetCapacity() error = nil, want the configured SetErr")
	}
	// A failed call must not silently update capacity.
	got, err := f.CurrentCapacity(context.Background())
	if err != nil {
		t.Fatalf("CurrentCapacity() error = %v, want nil", err)
	}
	if got != 4 {
		t.Fatalf("CurrentCapacity() = %d, want unchanged 4 after a failed SetCapacity", got)
	}
}
