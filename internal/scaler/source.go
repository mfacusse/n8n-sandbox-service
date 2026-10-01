package scaler

import (
	"context"
	"time"
)

// Runner is the scaler's own view of a registered runner's capacity data.
// Deliberately independent of registry.Runner (internal/api/registry), even
// though the fields mirror it: this package has zero dependency on
// internal/api, consistent with the scaler being its own component rather
// than a subsystem of the API.
type Runner struct {
	ID            string
	Healthy       bool
	CapacityTotal int32
	CapacityUsed  int32
	LastSeen      time.Time
}

// RunnerSource is how the scaler reads runner capacity data. The only
// implementation today (source_postgres.go) reads directly from the shared
// runner registry table — the same system of record the embedded scaler
// used to read in-process (Kubernetes Cluster Autoscaler precedent: read the
// orchestrator's own system of record directly, not through a metrics
// pipeline). The interface exists so that read mechanism is swappable later
// without touching decision logic.
type RunnerSource interface {
	All(ctx context.Context) ([]Runner, error)
}

// CapacitySignal is the aggregate view of runner free capacity used as the
// scaler's input, derived fresh from the RunnerSource every evaluation.
type CapacitySignal struct {
	ObservedAt         time.Time
	FreeCapacity       int
	TotalCapacity      int
	HealthyRunnerCount int
	// Available is false when no runner qualifies as healthy and fresh (a
	// fully stale/unreachable registry): that case must not be treated as a
	// zero-capacity (maximum urgency) signal.
	Available bool
}

// aggregateCapacitySignal sums free/total capacity across runners that are
// both marked healthy and within heartbeatGrace of now.
func aggregateCapacitySignal(ctx context.Context, source RunnerSource, heartbeatGrace time.Duration, now time.Time) (CapacitySignal, error) {
	runners, err := source.All(ctx)
	if err != nil {
		return CapacitySignal{ObservedAt: now}, err
	}

	sig := CapacitySignal{ObservedAt: now}
	for _, run := range runners {
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
	return sig, nil
}
