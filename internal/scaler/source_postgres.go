package scaler

import (
	"context"
	"database/sql"
	"fmt"
)

// PostgresRunnerSource reads runner capacity data directly from the shared
// `runners` table — the same table the API's Postgres registry backend
// (internal/api/registry.PostgresRegistry) uses, read here through the
// scaler's own connection rather than importing anything from internal/api.
// Selects only the columns the capacity signal needs (narrower than the
// API's equivalent query, which also carries routing fields irrelevant here).
type PostgresRunnerSource struct {
	db *sql.DB
}

// NewPostgresRunnerSource wraps an existing *sql.DB connection.
func NewPostgresRunnerSource(db *sql.DB) *PostgresRunnerSource {
	return &PostgresRunnerSource{db: db}
}

func (p *PostgresRunnerSource) All(ctx context.Context) ([]Runner, error) {
	const q = `
		SELECT id, healthy, capacity_total, capacity_used, last_seen
		FROM runners
		ORDER BY id`
	rows, err := p.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("scaler: query runners: %w", err)
	}
	defer rows.Close()

	var out []Runner
	for rows.Next() {
		var r Runner
		if err := rows.Scan(&r.ID, &r.Healthy, &r.CapacityTotal, &r.CapacityUsed, &r.LastSeen); err != nil {
			return nil, fmt.Errorf("scaler: scan runner row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scaler: iterate runner rows: %w", err)
	}
	return out, nil
}
