package scaler

import "context"

// FakeRunnerSource is an in-memory RunnerSource for tests.
type FakeRunnerSource struct {
	Runners []Runner
	Err     error
}

func (f *FakeRunnerSource) All(_ context.Context) ([]Runner, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return f.Runners, nil
}
