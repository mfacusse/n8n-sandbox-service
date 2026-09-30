package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// testScalerConfig uses a generous HeartbeatGrace: these tests advance the
// synthetic `now` passed to Evaluate by minutes to exercise cooldown/sustained
// -window logic, while seedRunner always stamps LastSeen with the real wall
// clock. A short grace would make runners look stale purely from that gap,
// which is a distinct scenario already covered by
// TestEvaluateStaleRegistryDoesNotScaleOut.
func testScalerConfig(policy config.ScalerConfig) *config.APIConfig {
	return &config.APIConfig{
		HeartbeatGrace: 24 * time.Hour,
		Scaler:         &policy,
	}
}

func newTestScaler(t *testing.T, policy config.ScalerConfig, reg registry.RunnerRegistry, initialCapacity int) (*CapacityScaler, *azurescale.Fake) {
	t.Helper()
	fake := azurescale.NewFake(initialCapacity)
	return NewCapacityScaler(testScalerConfig(policy), reg, fake, metrics.NewScalerRecorder(false)), fake
}

func seedRunner(t *testing.T, reg registry.RunnerRegistry, id string, total, used int32) {
	t.Helper()
	reg.Upsert(id, "http://127.0.0.1:8080", "127.0.0.1:9091", true, total, used, 0)
}

var basePolicy = config.ScalerConfig{
	MinNodes:            2,
	MaxNodes:            10,
	ScaleOutThreshold:   5,
	ScaleInThreshold:    30,
	ScaleInSustainedFor: 10 * time.Minute,
	Cooldown:            5 * time.Minute,
	EvalInterval:        time.Minute,
}

func TestEvaluateScaleOutBelowThresholdCappedAtMax(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8) // free = 2, below threshold 5

	scaler, fake := newTestScaler(t, basePolicy, reg, 4)
	now := time.Now()

	rec := scaler.Evaluate(context.Background(), now)

	if rec.Decision != DecisionScaleOut {
		t.Fatalf("Decision = %q, want %q (reason=%q)", rec.Decision, DecisionScaleOut, rec.Reason)
	}
	if rec.TargetNodeCount == nil || *rec.TargetNodeCount != 5 {
		t.Fatalf("TargetNodeCount = %v, want 5", rec.TargetNodeCount)
	}
	if got := fake.CallCount(); got != 1 {
		t.Fatalf("SetCapacity call count = %d, want 1", got)
	}
	if target, _ := fake.LastTarget(); target != 5 {
		t.Fatalf("SetCapacity target = %d, want 5", target)
	}
}

func TestEvaluateNoScaleOutAtMaxNodes(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8) // free = 2, below threshold 5

	scaler, fake := newTestScaler(t, basePolicy, reg, basePolicy.MaxNodes)
	rec := scaler.Evaluate(context.Background(), time.Now())

	if rec.Decision != DecisionNoOp || rec.Reason != ReasonAtMaxNodes {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q", rec.Decision, rec.Reason, ReasonAtMaxNodes)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0", got)
	}
}

func TestEvaluateScaleOutDeferredWithinCooldown(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8) // free = 2, below threshold 5

	scaler, fake := newTestScaler(t, basePolicy, reg, 4)
	now := time.Now()

	first := scaler.Evaluate(context.Background(), now)
	if first.Decision != DecisionScaleOut {
		t.Fatalf("first evaluation decision = %q, want scale_out", first.Decision)
	}

	// Fake's capacity moved to 5; keep runner capacity the same so free capacity
	// is still below threshold on the second pass.
	second := scaler.Evaluate(context.Background(), now.Add(1*time.Second))
	if second.Decision != DecisionNoOp || second.Reason != ReasonCooldownActive {
		t.Fatalf("second evaluation = decision=%q reason=%q, want no_op/%q", second.Decision, second.Reason, ReasonCooldownActive)
	}
	if got := fake.CallCount(); got != 1 {
		t.Fatalf("SetCapacity call count after cooldown-blocked cycle = %d, want 1 (unchanged)", got)
	}
}

func TestEvaluateStaleRegistryDoesNotScaleOut(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second) // no runners registered at all
	scaler, fake := newTestScaler(t, basePolicy, reg, 4)

	rec := scaler.Evaluate(context.Background(), time.Now())

	if rec.SignalAvailable {
		t.Fatalf("SignalAvailable = true, want false for an empty/stale registry")
	}
	if rec.Decision != DecisionNoOp || rec.Reason != ReasonRegistryStale {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q", rec.Decision, rec.Reason, ReasonRegistryStale)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0 (must not scale out on missing data)", got)
	}
}

func TestEvaluateScaleInAfterSustainedExcess(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 100, 10) // free = 90, above threshold 30

	scaler, fake := newTestScaler(t, basePolicy, reg, 6)
	t0 := time.Now()

	first := scaler.Evaluate(context.Background(), t0)
	if first.Decision != DecisionNoOp || first.Reason != ReasonNotYetSustained {
		t.Fatalf("first evaluation = decision=%q reason=%q, want no_op/%q", first.Decision, first.Reason, ReasonNotYetSustained)
	}

	second := scaler.Evaluate(context.Background(), t0.Add(basePolicy.ScaleInSustainedFor+time.Second))
	if second.Decision != DecisionScaleIn {
		t.Fatalf("second evaluation Decision = %q, want scale_in (reason=%q)", second.Decision, second.Reason)
	}
	if second.TargetNodeCount == nil || *second.TargetNodeCount != 5 {
		t.Fatalf("TargetNodeCount = %v, want 5", second.TargetNodeCount)
	}
	if got := fake.CallCount(); got != 1 {
		t.Fatalf("SetCapacity call count = %d, want 1", got)
	}
}

func TestEvaluateNoScaleInAtMinNodes(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 100, 10) // free = 90, above threshold 30

	scaler, fake := newTestScaler(t, basePolicy, reg, basePolicy.MinNodes)
	t0 := time.Now()
	scaler.Evaluate(context.Background(), t0) // starts the sustained window

	rec := scaler.Evaluate(context.Background(), t0.Add(basePolicy.ScaleInSustainedFor+time.Second))
	if rec.Decision != DecisionNoOp || rec.Reason != ReasonAtMinNodes {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q", rec.Decision, rec.Reason, ReasonAtMinNodes)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0", got)
	}
}

func TestEvaluateScaleInDeferredWithinCooldown(t *testing.T) {
	policy := basePolicy
	policy.ScaleInSustainedFor = 0 // isolate cooldown behavior from the sustained-window check
	policy.Cooldown = 5 * time.Minute

	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8) // free = 2, below scale-out threshold: triggers a scale-out first
	scaler, fake := newTestScaler(t, policy, reg, 4)
	t0 := time.Now()

	first := scaler.Evaluate(context.Background(), t0)
	if first.Decision != DecisionScaleOut {
		t.Fatalf("setup: first evaluation decision = %q, want scale_out", first.Decision)
	}

	// Now flip to excess capacity, still within the cooldown window.
	reg.Upsert("r1", "http://127.0.0.1:8080", "127.0.0.1:9091", true, 100, 10, 0) // free = 90
	second := scaler.Evaluate(context.Background(), t0.Add(1*time.Second))
	if second.Decision != DecisionNoOp || second.Reason != ReasonCooldownActive {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q", second.Decision, second.Reason, ReasonCooldownActive)
	}
	if got := fake.CallCount(); got != 1 {
		t.Fatalf("SetCapacity call count = %d, want 1 (unchanged since setup)", got)
	}
}

func TestEvaluateScaleInBlipResetsSustainedWindow(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	scaler, fake := newTestScaler(t, basePolicy, reg, 6)
	t0 := time.Now()

	seedRunner(t, reg, "r1", 100, 10) // free = 90, above threshold 30: condition starts
	if rec := scaler.Evaluate(context.Background(), t0); rec.Reason != ReasonNotYetSustained {
		t.Fatalf("t0 reason = %q, want %q", rec.Reason, ReasonNotYetSustained)
	}

	seedRunner(t, reg, "r1", 100, 80) // free = 20, within thresholds: the blip
	if rec := scaler.Evaluate(context.Background(), t0.Add(time.Second)); rec.Reason != ReasonWithinThresholds {
		t.Fatalf("t0+1s reason = %q, want %q", rec.Reason, ReasonWithinThresholds)
	}

	seedRunner(t, reg, "r1", 100, 10) // free = 90 again: a fresh sustained window
	rec := scaler.Evaluate(context.Background(), t0.Add(basePolicy.ScaleInSustainedFor+2*time.Second))
	if rec.Decision != DecisionNoOp || rec.Reason != ReasonNotYetSustained {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q (blip must reset the sustained-window start)", rec.Decision, rec.Reason, ReasonNotYetSustained)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0", got)
	}
}

func TestEvaluateMinEqualsMaxDisablesScaling(t *testing.T) {
	policy := basePolicy
	policy.MinNodes = 5
	policy.MaxNodes = 5

	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 9) // free = 1, deep below scale-out threshold

	scaler, fake := newTestScaler(t, policy, reg, 5)
	rec := scaler.Evaluate(context.Background(), time.Now())

	if rec.Decision != DecisionNoOp || rec.Reason != ReasonScalingDisabled {
		t.Fatalf("got decision=%q reason=%q, want no_op/%q", rec.Decision, rec.Reason, ReasonScalingDisabled)
	}
	if !rec.SignalAvailable || rec.FreeCapacity != 1 {
		t.Fatalf("signal not recorded despite scaling being disabled: available=%v free=%d", rec.SignalAvailable, rec.FreeCapacity)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0", got)
	}
}

func TestEvaluateConflictingThresholdsScaleOutTakesPrecedence(t *testing.T) {
	policy := basePolicy
	policy.ScaleOutThreshold = 30 // free < 30 triggers scale-out
	policy.ScaleInThreshold = 5   // free > 5 triggers scale-in

	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 100, 85) // free = 15: satisfies BOTH conditions under this misconfiguration

	scaler, fake := newTestScaler(t, policy, reg, 4)
	rec := scaler.Evaluate(context.Background(), time.Now())

	if rec.Decision != DecisionScaleOut {
		t.Fatalf("Decision = %q, want scale_out (precedence over scale_in) — reason=%q", rec.Decision, rec.Reason)
	}
	if got := fake.CallCount(); got != 1 {
		t.Fatalf("SetCapacity call count = %d, want exactly 1 (never both directions in one cycle)", got)
	}
}

func TestLastDecisionReflectsMostRecentEvaluation(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	scaler, _ := newTestScaler(t, basePolicy, reg, 4)

	if got := scaler.LastDecision(); got != nil {
		t.Fatalf("LastDecision() before any evaluation = %+v, want nil", got)
	}

	seedRunner(t, reg, "r1", 10, 8) // free = 2: scale-out
	scaler.Evaluate(context.Background(), time.Now())
	first := scaler.LastDecision()
	if first == nil || first.Decision != DecisionScaleOut {
		t.Fatalf("LastDecision() after first evaluation = %+v, want scale_out", first)
	}

	seedRunner(t, reg, "r1", 10, 9) // still low, but now blocked by cooldown
	scaler.Evaluate(context.Background(), time.Now().Add(time.Second))
	second := scaler.LastDecision()
	if second == nil || second.Decision != DecisionNoOp || second.Reason != ReasonCooldownActive {
		t.Fatalf("LastDecision() after second evaluation = %+v, want no_op/cooldown active", second)
	}

	// Reading twice must not itself change anything.
	if third := scaler.LastDecision(); third == nil || third.Reason != second.Reason {
		t.Fatalf("LastDecision() is not idempotent: got %+v then %+v", second, third)
	}
}

func TestEvaluateHandlesCurrentCapacityError(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8)

	fake := azurescale.NewFake(4)
	fake.GetErr = errors.New("azure: transient failure")
	scaler := NewCapacityScaler(testScalerConfig(basePolicy), reg, fake, metrics.NewScalerRecorder(false))

	rec := scaler.Evaluate(context.Background(), time.Now())
	if rec.Error == "" {
		t.Fatal("Error = \"\", want the CurrentCapacity failure surfaced")
	}
	if rec.Decision != DecisionNoOp {
		t.Fatalf("Decision = %q, want no_op when current capacity cannot be read", rec.Decision)
	}
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("SetCapacity call count = %d, want 0 (must not assume success or guess a target)", got)
	}
}

func TestEvaluateSetCapacityFailureDoesNotStartCooldown(t *testing.T) {
	reg := registry.NewMemory(45 * time.Second)
	seedRunner(t, reg, "r1", 10, 8) // free = 2, below threshold

	fake := azurescale.NewFake(4)
	fake.SetErr = errors.New("azure: rate limited")
	scaler := NewCapacityScaler(testScalerConfig(basePolicy), reg, fake, metrics.NewScalerRecorder(false))
	t0 := time.Now()

	first := scaler.Evaluate(context.Background(), t0)
	if first.Decision != DecisionScaleOut || first.Error == "" {
		t.Fatalf("first evaluation = decision=%q error=%q, want scale_out with an error recorded", first.Decision, first.Error)
	}

	// A failed action must not have started the cooldown: immediately retrying
	// should attempt again, not report cooldown active.
	fake.SetErr = nil
	second := scaler.Evaluate(context.Background(), t0.Add(time.Millisecond))
	if second.Decision != DecisionScaleOut || second.Error != "" {
		t.Fatalf("second evaluation = decision=%q error=%q, want a clean scale_out retry", second.Decision, second.Error)
	}
	if got := fake.CallCount(); got != 2 {
		t.Fatalf("SetCapacity call count = %d, want 2 (failed attempt + successful retry)", got)
	}
}
