package api

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/n8n-io/sandbox-service/internal/api/config"
	"github.com/n8n-io/sandbox-service/internal/api/registry"
	"github.com/n8n-io/sandbox-service/internal/api/store"
	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
)

// Scale decision outcomes and reasons. Reasons are also used verbatim in log
// lines and the GET /admin/scaler response, so they are exported as
// constants to keep the vocabulary consistent everywhere it appears.
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
)

// CapacitySignal is the aggregate view of runner free capacity used as the
// scaler's input, derived fresh from the runner registry every evaluation.
type CapacitySignal struct {
	ObservedAt         time.Time
	FreeCapacity       int
	TotalCapacity      int
	HealthyRunnerCount int
	// Available is false when no runner qualifies as healthy and fresh (a
	// fully stale/unreachable registry), per FR-011: that case must not be
	// treated as a zero-capacity (maximum urgency) signal.
	Available bool
}

// aggregateCapacitySignal sums free/total capacity across runners that are
// both marked healthy and within heartbeatGrace of now. heartbeatGrace comes
// from the caller's config rather than the registry, since RunnerRegistry
// does not expose the value it was constructed with.
func aggregateCapacitySignal(reg registry.RunnerRegistry, heartbeatGrace time.Duration, now time.Time) CapacitySignal {
	sig := CapacitySignal{ObservedAt: now}
	for _, run := range reg.All() {
		if !run.Healthy {
			continue
		}
		if now.Sub(run.LastSeen) > heartbeatGrace {
			continue
		}
		sig.HealthyRunnerCount++
		sig.TotalCapacity += int(run.CapacityTotal)
		free := int(run.CapacityTotal) - int(run.CapacityUsed)
		if free > 0 {
			sig.FreeCapacity += free
		}
	}
	sig.Available = sig.HealthyRunnerCount > 0
	return sig
}

// ScaleDecisionRecord is the outcome of one scaler evaluation cycle: what was
// observed, what was decided, and why. It is what GET /admin/scaler and the
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

// CapacityScaler periodically evaluates runner capacity and adjusts VMSS node
// count within a configured policy. It follows the same shape as the idle
// sandbox sweeper (ttl.go): a ticker-driven loop, gated by config, using the
// existing Postgres advisory lock for single-writer-per-cycle enforcement
// across API pods.
type CapacityScaler struct {
	reg    registry.RunnerRegistry
	cfg    *config.APIConfig
	policy config.ScalerConfig
	vmss   azurescale.VMSSScaler
	rec    *metrics.ScalerRecorder

	// lastAction and scaleInConditionSince are touched only from the single
	// evaluation goroutine (the ticker in Start, or a test calling Evaluate
	// directly) — never concurrently, so no mutex guards them.
	lastAction            time.Time
	scaleInConditionSince *time.Time

	mu           sync.Mutex
	lastDecision *ScaleDecisionRecord
}

// NewCapacityScaler builds a scaler for the given policy. cfg.Scaler must be
// non-nil; callers check that (and skip building a scaler at all) before
// calling this, mirroring LogIdleSweepConfig/StartIdleSweeper's own
// enabled-check pattern.
func NewCapacityScaler(cfg *config.APIConfig, reg registry.RunnerRegistry, vmss azurescale.VMSSScaler, rec *metrics.ScalerRecorder) *CapacityScaler {
	return &CapacityScaler{
		reg:    reg,
		cfg:    cfg,
		policy: *cfg.Scaler,
		vmss:   vmss,
		rec:    rec,
	}
}

// LogScalerConfig logs whether the capacity scaler runs and with which policy.
func LogScalerConfig(cfg *config.APIConfig) {
	if cfg.Scaler == nil {
		slog.Info("runner capacity scaler disabled")
		return
	}
	s := cfg.Scaler
	slog.Info("runner capacity scaler enabled",
		"min_nodes", s.MinNodes,
		"max_nodes", s.MaxNodes,
		"scale_out_threshold", s.ScaleOutThreshold,
		"scale_in_threshold", s.ScaleInThreshold,
		"scale_in_sustained_for", s.ScaleInSustainedFor.String(),
		"cooldown", s.Cooldown.String(),
		"eval_interval", s.EvalInterval.String(),
		"vmss_resource_group", s.AzureResourceGroup,
		"vmss_name", s.AzureVMSSName)
}

// Start runs Evaluate on a ticker until ctx is done. When lockDB is non-nil
// (Postgres multi-pod), only the advisory-lock holder evaluates each cycle,
// exactly like StartIdleSweeper.
func (c *CapacityScaler) Start(ctx context.Context, lockDB *sql.DB) {
	interval := c.policy.EvalInterval
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
				runEval := func() error {
					c.Evaluate(ctx, time.Now())
					return nil
				}
				if lockDB != nil {
					ran, err := store.TryRun(ctx, lockDB, runEval)
					if err != nil {
						slog.Error("scaler evaluation failed", "err", err)
					} else if !ran {
						slog.Debug("scaler evaluation skipped: another pod holds the lock")
					}
				} else {
					_ = runEval()
				}
			}
		}
	}()
}

// Evaluate runs one evaluation cycle: read the capacity signal, decide, act,
// log, and record metrics. It is exported (rather than kept behind Start's
// ticker) so tests can drive it directly and deterministically.
func (c *CapacityScaler) Evaluate(ctx context.Context, now time.Time) ScaleDecisionRecord {
	signal := aggregateCapacitySignal(c.reg, c.cfg.HeartbeatGrace, now)

	currentCount, err := c.vmss.CurrentCapacity(ctx)
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
		c.finish(rec)
		return rec
	}

	decision, reason, target, hasTarget := c.decide(signal, currentCount, now)
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
		// runner behind that instance — the registry has no VMSS-instance-ID
		// linkage to a runner ID. FR-010 (drain before removal) is therefore
		// only partially satisfied: in-flight sandboxes on whichever instance
		// Azure happens to remove are not proactively relocated. See
		// docs/scaler-production-hardening.md.
		if err := c.vmss.SetCapacity(ctx, target); err != nil {
			rec.Error = err.Error()
		} else {
			c.lastAction = now
			if decision == DecisionScaleIn {
				c.scaleInConditionSince = nil
			}
		}
	}

	c.finish(rec)
	return rec
}

// decide applies the scaling policy to one signal/current-count observation.
// Scale-out is evaluated before scale-in (deterministic precedence): if both
// conditions are somehow true in the same cycle — only reachable via a
// misconfiguration where the scale-in threshold does not exceed the scale-out
// threshold — scale-out wins and scale-in is not evaluated at all this cycle.
func (c *CapacityScaler) decide(signal CapacitySignal, currentCount int, now time.Time) (decision, reason string, target int, hasTarget bool) {
	if !signal.Available {
		return DecisionNoOp, ReasonRegistryStale, 0, false
	}
	if c.policy.MinNodes == c.policy.MaxNodes {
		return DecisionNoOp, ReasonScalingDisabled, 0, false
	}

	if signal.FreeCapacity < c.policy.ScaleOutThreshold {
		if currentCount >= c.policy.MaxNodes {
			return DecisionNoOp, ReasonAtMaxNodes, 0, false
		}
		if !c.cooldownElapsed(now) {
			return DecisionNoOp, ReasonCooldownActive, 0, false
		}
		target := currentCount + 1
		if target > c.policy.MaxNodes {
			target = c.policy.MaxNodes
		}
		return DecisionScaleOut, ReasonBelowScaleOutThresh, target, true
	}

	if signal.FreeCapacity > c.policy.ScaleInThreshold {
		if c.scaleInConditionSince == nil {
			since := now
			c.scaleInConditionSince = &since
		}
		if now.Sub(*c.scaleInConditionSince) < c.policy.ScaleInSustainedFor {
			return DecisionNoOp, ReasonNotYetSustained, 0, false
		}
		if currentCount <= c.policy.MinNodes {
			return DecisionNoOp, ReasonAtMinNodes, 0, false
		}
		if !c.cooldownElapsed(now) {
			return DecisionNoOp, ReasonCooldownActive, 0, false
		}
		target := currentCount - 1
		if target < c.policy.MinNodes {
			target = c.policy.MinNodes
		}
		return DecisionScaleIn, ReasonAboveScaleInSustained, target, true
	}

	// Neither condition holds: reset the sustained-excess tracker so a later
	// excess period starts its own fresh sustained window rather than reusing
	// a stale start time (the single-below-threshold-blip edge case).
	c.scaleInConditionSince = nil
	return DecisionNoOp, ReasonWithinThresholds, 0, false
}

// cooldownElapsed reports whether enough time has passed since the last
// successful scale action. A zero lastAction (process just started, or no
// action has ever succeeded) is treated as elapsed.
func (c *CapacityScaler) cooldownElapsed(now time.Time) bool {
	if c.lastAction.IsZero() {
		return true
	}
	return now.Sub(c.lastAction) >= c.policy.Cooldown
}

// finish records the decision for GET /admin/scaler, logs it, and updates
// metrics. Always called exactly once per Evaluate, success or failure.
func (c *CapacityScaler) finish(rec ScaleDecisionRecord) {
	c.mu.Lock()
	r := rec
	c.lastDecision = &r
	c.mu.Unlock()

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

	if c.rec == nil || !c.rec.Enabled() {
		return
	}
	metricDecision := rec.Decision
	if rec.Error != "" {
		metricDecision = metrics.ScaleDecisionError
	}
	c.rec.ObserveDecision(metricDecision, rec.CurrentNodeCount, rec.FreeCapacity)
}

// LastDecision returns the most recent evaluation's outcome, or nil if no
// evaluation has completed yet. Safe for concurrent use with Evaluate.
func (c *CapacityScaler) LastDecision() *ScaleDecisionRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastDecision == nil {
		return nil
	}
	r := *c.lastDecision
	return &r
}

// Policy returns the scaler's active policy, for GET /admin/scaler.
func (c *CapacityScaler) Policy() config.ScalerConfig { return c.policy }
