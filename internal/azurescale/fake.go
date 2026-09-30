package azurescale

import (
	"context"
	"sync"
)

// Fake is an in-memory VMSSScaler for tests. Capacity starts at Initial and is
// updated by SetCapacity; every call is recorded for assertions.
type Fake struct {
	mu sync.Mutex

	capacity int

	// GetErr, if set, is returned by CurrentCapacity instead of the stored value.
	GetErr error
	// SetErr, if set, is returned by SetCapacity instead of applying target.
	SetErr error

	// Calls records every target passed to SetCapacity, in order.
	Calls []int
}

// NewFake returns a Fake VMSSScaler starting at the given capacity.
func NewFake(initial int) *Fake {
	return &Fake{capacity: initial}
}

func (f *Fake) CurrentCapacity(_ context.Context) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.GetErr != nil {
		return 0, f.GetErr
	}
	return f.capacity, nil
}

func (f *Fake) SetCapacity(_ context.Context, target int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, target)
	if f.SetErr != nil {
		return f.SetErr
	}
	f.capacity = target
	return nil
}

// CallCount returns how many times SetCapacity has been called. Intended for
// tests in other packages.
func (f *Fake) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Calls)
}

// LastTarget returns the most recent SetCapacity target, or (0, false) if
// SetCapacity has never been called.
func (f *Fake) LastTarget() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.Calls) == 0 {
		return 0, false
	}
	return f.Calls[len(f.Calls)-1], true
}
