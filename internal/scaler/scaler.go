// Package scaler implements the runner-VMSS capacity scaler as a standalone
// component: it periodically reads the runner capacity signal (via a
// RunnerSource), decides whether to scale the target VMSS, and acts through
// an azurescale.VMSSScaler. It has no dependency on internal/api — this
// package is what used to run in-process inside the API gateway
// (001-runner-scaler-vmss) and now runs as its own binary (cmd/scaler).
package scaler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// Scale decision outcomes and reasons. Reasons are also used verbatim in log
// lines and the GET /policy response, so they are exported as constants to
// keep the vocabulary consistent everywhere it appears.
const (
	DecisionScaleOut = "scale_out"
	DecisionScaleIn  = "scale_in"
	DecisionNoOp     = "no_op"

	ReasonAtMaxNodes            = "at max nodes"
	ReasonAtMinNodes            = "at min nodes"
	ReasonCooldownActive        = "cooldown active"
	ReasonRegistryStale         = "registry stale"
	ReasonScalingDisabled       = "min==max, scaling disabled"
	ReasonWithinThresholds      = "within thresholds"
	ReasonNotYetSustained       = "excess capacity not yet sustained"
	ReasonBelowScaleOutThresh   = "below scale-out threshold"
	ReasonAboveScaleInSustained = "sustained excess capacity above scale-in threshold"
	ReasonCurrentCapacityFailed = "failed to read current VMSS capacity"
	ReasonSignalSourceFailed    = "failed to read runner capacity signal"
)

// Policy is the scaling policy and VMSS target — what GET /policy reports.
type Policy struct {
	MinNodes            int
	MaxNodes            int
	ScaleOutThreshold   int
	ScaleInThreshold    int
	ScaleInSustainedFor time.Duration
	Cooldown            time.Duration
	EvalInterval        time.Duration
	AzureSubscriptionID string
	AzureResourceGroup  string
	AzureVMSSName       string
}

// ScaleDecisionRecord is the outcome of one scaler evaluation cycle: what was
// observed, what was decided, and why. It is what GET /policy and the
// scaler's log line both report.
type ScaleDecisionRecord struct {
	ObservedAt       time.Time
	FreeCapacity     int
	TotalCapacity    int
	SignalAvailable  bool
	CurrentNodeCount int
	Decision         string
	Reason           string
	TargetNodeCount  *int
	Error            string
}

// Scaler periodically evaluates runner capacity and adjusts VMSS node count
// within a configured policy. Runs as a single replica (FR-008) — no
// locking/leader-election: at-most-one-active-evaluation is guaranteed
// structurally by there being exactly one instance, not by coordination.
type Scaler struct {
	source         RunnerSource
	heartbeatGrace time.Duration
	policy         Policy
	vmss           azurescale.VMSSScaler
	rec            *metrics.ScalerRecorder

	// lastAction and scaleInConditionSince are touched only from the single
	// evaluation goroutine (the ticker in Start, or a test calling Evaluate
	// directly) — never concurrently, so no mutex guards them.
	lastAction            time.Time
	scaleInConditionSince *time.Time

	mu           sync.Mutex
	lastDecision *ScaleDecisionRecord
}

// New builds a Scaler for the given policy.
func New(policy Policy, heartbeatGrace time.Duration, source RunnerSource, vmss azurescale.VMSSScaler, rec *metrics.ScalerRecorder) *Scaler {
	return &Scaler{
		source:         source,
		heartbeatGrace: heartbeatGrace,
		policy:         policy,
		vmss:           vmss,
		rec:            rec,
	}
}

// LogConfig logs the active scaling policy at startup.
func LogConfig(p Policy) {
	slog.Info("runner capacity scaler starting",
		"min_nodes", p.MinNodes,
		"max_nodes", p.MaxNodes,
		"scale_out_threshold", p.ScaleOutThreshold,
		"scale_in_threshold", p.ScaleInThreshold,
		"scale_in_sustained_for", p.ScaleInSustainedFor.String(),
		"cooldown", p.Cooldown.String(),
		"eval_interval", p.EvalInterval.String(),
		"vmss_resource_group", p.AzureResourceGroup,
		"vmss_name", p.AzureVMSSName)
}

// Start runs Evaluate on a ticker until ctx is done.
func (s *Scaler) Start(ctx context.Context) {
	interval := s.policy.EvalInterval
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s.Evaluate(ctx, time.Now())
			}
		}
	}()
}

// Evaluate runs one evaluation cycle: read the capacity signal, decide, act,
// log, and record metrics. It is exported (rather than kept behind Start's
// ticker) so tests can drive it directly and deterministically.
func (s *Scaler) Evaluate(ctx context.Context, now time.Time) ScaleDecisionRecord {
	signal, err := aggregateCapacitySignal(ctx, s.source, s.heartbeatGrace, now)
	if err != nil {
		rec := ScaleDecisionRecord{
			ObservedAt: now,
			Decision:   DecisionNoOp,
			Reason:     ReasonSignalSourceFailed,
			Error:      err.Error(),
		}
		s.finish(rec)
		return rec
	}

	currentCount, err := s.vmss.CurrentCapacity(ctx)
	if err != nil {
		rec := ScaleDecisionRecord{
			ObservedAt:      now,
			FreeCapacity:    signal.FreeCapacity,
			TotalCapacity:   signal.TotalCapacity,
			SignalAvailable: signal.Available,
			Decision:        DecisionNoOp,
			Reason:          ReasonCurrentCapacityFailed,
			Error:           err.Error(),
		}
		s.finish(rec)
		return rec
	}

	decision, reason, target, hasTarget := s.decide(signal, currentCount, now)
	rec := ScaleDecisionRecord{
		ObservedAt:       now,
		FreeCapacity:     signal.FreeCapacity,
		TotalCapacity:    signal.TotalCapacity,
		SignalAvailable:  signal.Available,
		CurrentNodeCount: currentCount,
		Decision:         decision,
		Reason:           reason,
	}
	if hasTarget {
		t := target
		rec.TargetNodeCount = &t
	}

	if decision != DecisionNoOp && hasTarget {
		// KNOWN GAP (tracked for the CP-2690 production-hardening pass): this
		// requests a capacity decrease from Azure but cannot choose which VMSS
		// instance is removed, and has no way to first drain the specific
		// runner behind that instance — there is no VMSS-instance-ID linkage
		// to a runner ID. Drain-before-removal is therefore only partially
		// satisfied: in-flight sandboxes on whichever instance Azure happens
		// to remove are not proactively relocated. See
		// docs/scaler-production-hardening.md.
		if err := s.vmss.SetCapacity(ctx, target); err != nil {
			rec.Error = err.Error()
		} else {
			s.lastAction = now
			if decision == DecisionScaleIn {
				s.scaleInConditionSince = nil
			}
		}
	}

	s.finish(rec)
	return rec
}

// decide applies the scaling policy to one signal/current-count observation.
// Scale-out is evaluated before scale-in (deterministic precedence): if both
// conditions are somehow true in the same cycle — only reachable via a
// misconfiguration where the scale-in threshold does not exceed the scale-out
// threshold — scale-out wins and scale-in is not evaluated at all this cycle.
func (s *Scaler) decide(signal CapacitySignal, currentCount int, now time.Time) (decision, reason string, target int, hasTarget bool) {
	if !signal.Available {
		return DecisionNoOp, ReasonRegistryStale, 0, false
	}
	if s.policy.MinNodes == s.policy.MaxNodes {
		return DecisionNoOp, ReasonScalingDisabled, 0, false
	}

	if signal.FreeCapacity < s.policy.ScaleOutThreshold {
		if currentCount >= s.policy.MaxNodes {
			return DecisionNoOp, ReasonAtMaxNodes, 0, false
		}
		if !s.cooldownElapsed(now) {
			return DecisionNoOp, ReasonCooldownActive, 0, false
		}
		target := currentCount + 1
		// Defensive, not currently reachable: the guard above already ensures
		// currentCount < MaxNodes, so target <= MaxNodes given the fixed +1
		// step. Kept so a future change to the step size (e.g. a variable
		// step) stays bounded without relying on every future caller to
		// re-derive this invariant.
		if target > s.policy.MaxNodes {
			target = s.policy.MaxNodes
		}
		return DecisionScaleOut, ReasonBelowScaleOutThresh, target, true
	}

	if signal.FreeCapacity > s.policy.ScaleInThreshold {
		if s.scaleInConditionSince == nil {
			since := now
			s.scaleInConditionSince = &since
		}
		if now.Sub(*s.scaleInConditionSince) < s.policy.ScaleInSustainedFor {
			return DecisionNoOp, ReasonNotYetSustained, 0, false
		}
		if currentCount <= s.policy.MinNodes {
			return DecisionNoOp, ReasonAtMinNodes, 0, false
		}
		if !s.cooldownElapsed(now) {
			return DecisionNoOp, ReasonCooldownActive, 0, false
		}
		target := currentCount - 1
		// Defensive, not currently reachable: symmetric with the scale-out
		// clamp above (the guard already ensures currentCount > MinNodes).
		if target < s.policy.MinNodes {
			target = s.policy.MinNodes
		}
		return DecisionScaleIn, ReasonAboveScaleInSustained, target, true
	}

	// Neither condition holds: reset the sustained-excess tracker so a later
	// excess period starts its own fresh sustained window rather than reusing
	// a stale start time (the single-below-threshold-blip edge case).
	s.scaleInConditionSince = nil
	return DecisionNoOp, ReasonWithinThresholds, 0, false
}

// cooldownElapsed reports whether enough time has passed since the last
// successful scale action. A zero lastAction (process just started, or no
// action has ever succeeded) is treated as elapsed.
func (s *Scaler) cooldownElapsed(now time.Time) bool {
	if s.lastAction.IsZero() {
		return true
	}
	return now.Sub(s.lastAction) >= s.policy.Cooldown
}

// finish records the decision for GET /policy, logs it, and updates metrics.
// Always called exactly once per Evaluate, success or failure.
func (s *Scaler) finish(rec ScaleDecisionRecord) {
	s.mu.Lock()
	r := rec
	s.lastDecision = &r
	s.mu.Unlock()

	args := []any{
		"free_capacity", rec.FreeCapacity,
		"total_capacity", rec.TotalCapacity,
		"signal_available", rec.SignalAvailable,
		"current_node_count", rec.CurrentNodeCount,
		"decision", rec.Decision,
		"reason", rec.Reason,
	}
	if rec.TargetNodeCount != nil {
		args = append(args, "target_node_count", *rec.TargetNodeCount)
	}
	if rec.Error != "" {
		args = append(args, "error", rec.Error)
		slog.Error("scaler evaluation", args...)
	} else {
		slog.Info("scaler evaluation", args...)
	}

	if s.rec == nil || !s.rec.Enabled() {
		return
	}
	metricDecision := rec.Decision
	if rec.Error != "" {
		metricDecision = metrics.ScaleDecisionError
	}
	// A signal-source failure means neither value was observed this cycle; a
	// CurrentCapacity failure means the capacity signal was read but the node
	// count was not. Either way, the unread gauge must not be overwritten
	// with zero.
	freeCapacityValid := rec.Reason != ReasonSignalSourceFailed
	nodeCountValid := freeCapacityValid && rec.Reason != ReasonCurrentCapacityFailed
	s.rec.ObserveDecision(metricDecision, nodeCountValid, rec.CurrentNodeCount, freeCapacityValid, rec.FreeCapacity)
}

// LastDecision returns the most recent evaluation's outcome, or nil if no
// evaluation has completed yet. Safe for concurrent use with Evaluate.
func (s *Scaler) LastDecision() *ScaleDecisionRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastDecision == nil {
		return nil
	}
	r := *s.lastDecision
	return &r
}

// Policy returns the scaler's active policy, for GET /policy.
func (s *Scaler) Policy() Policy { return s.policy }
