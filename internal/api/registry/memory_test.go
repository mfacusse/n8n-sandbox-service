package registry

import (
	"errors"
	"testing"
	"time"
)

func TestPickLowestUsedSelectsLowestCapacity(t *testing.T) {
	reg := NewMemory(45 * time.Second)
	reg.Upsert("r-high", "http://127.0.0.1:8080", "127.0.0.1:9091", true, 10, 5, 0)
	reg.Upsert("r-low", "http://127.0.0.1:8081", "127.0.0.1:9092", true, 10, 1, 0)
	reg.Upsert("r-tie", "http://127.0.0.1:8082", "127.0.0.1:9093", true, 10, 1, 0)

	run, err := reg.PickLowestUsed()
	if err != nil {
		t.Fatalf("PickLowestUsed() failed: %v", err)
	}
	if run.ID != "r-low" {
		t.Fatalf("PickLowestUsed() = %q, want r-low (lowest id at equal capacity)", run.ID)
	}
}

func TestPickLowestUsedNoEligibleRunners(t *testing.T) {
	reg := NewMemory(45 * time.Second)
	reg.Upsert("r1", "http://127.0.0.1:8080", "127.0.0.1:9091", true, 1, 1, 0)

	if _, err := reg.PickLowestUsed(); !errors.Is(err, ErrNoRunners) {
		t.Fatalf("PickLowestUsed() error = %v, want ErrNoRunners", err)
	}
}

func TestAllReturnsEveryRunnerRegardlessOfHealth(t *testing.T) {
	reg := NewMemory(45 * time.Second)
	reg.Upsert("r-healthy", "http://127.0.0.1:8080", "127.0.0.1:9091", true, 10, 3, 0)
	reg.Upsert("r-unhealthy", "http://127.0.0.1:8081", "127.0.0.1:9092", false, 10, 0, 0)

	all, err := reg.All()
	if err != nil {
		t.Fatalf("All() error = %v, want nil", err)
	}
	if len(all) != 2 {
		t.Fatalf("All() returned %d runners, want 2", len(all))
	}
	byID := make(map[string]Runner, len(all))
	for _, r := range all {
		byID[r.ID] = r
	}
	if got, ok := byID["r-healthy"]; !ok || got.CapacityUsed != 3 {
		t.Fatalf("All() missing or wrong data for r-healthy: %+v ok=%v", got, ok)
	}
	if got, ok := byID["r-unhealthy"]; !ok || got.Healthy {
		t.Fatalf("All() should include unhealthy runners as-is: %+v ok=%v", got, ok)
	}

	reg.Remove("r-unhealthy")
	afterRemove, err := reg.All()
	if err != nil {
		t.Fatalf("All() after Remove() error = %v, want nil", err)
	}
	if len(afterRemove) != 1 {
		t.Fatalf("All() after Remove() = %d, want 1", len(afterRemove))
	}
}
