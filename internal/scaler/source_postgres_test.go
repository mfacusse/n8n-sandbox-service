//go:build integration

package scaler

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func openTestPostgresSource(t *testing.T) (*PostgresRunnerSource, *sql.DB) {
	t.Helper()
	host := os.Getenv("SANDBOX_TEST_POSTGRES_HOST")
	if host == "" {
		t.Skip("SANDBOX_TEST_POSTGRES_HOST not set")
	}
	port := 5432
	if v := os.Getenv("SANDBOX_TEST_POSTGRES_PORT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("SANDBOX_TEST_POSTGRES_PORT: %v", err)
		}
		port = n
	}
	dsn := "host=" + host + " port=" + strconv.Itoa(port) +
		" user=" + os.Getenv("SANDBOX_TEST_POSTGRES_USER") +
		" password=" + os.Getenv("SANDBOX_TEST_POSTGRES_PASSWORD") +
		" dbname=" + os.Getenv("SANDBOX_TEST_POSTGRES_DB") +
		" sslmode=disable"
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewPostgresRunnerSource(db), db
}

func TestPostgresRunnerSourceAllReturnsSeededRows(t *testing.T) {
	source, db := openTestPostgresSource(t)
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM runners WHERE id LIKE 'scaler-pg-test-%'`) })

	_, err := db.Exec(`
		INSERT INTO runners (id, http_base_url, control_grpc_addr, healthy, capacity_total, capacity_used, capacity_stopped, last_seen)
		VALUES ('scaler-pg-test-1', 'http://127.0.0.1:8080', '127.0.0.1:9091', true, 10, 3, 0, now())
		ON CONFLICT (id) DO UPDATE SET capacity_total = EXCLUDED.capacity_total, capacity_used = EXCLUDED.capacity_used`)
	if err != nil {
		t.Fatalf("seed runner: %v", err)
	}

	runners, err := source.All(context.Background())
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	found := false
	for _, r := range runners {
		if r.ID == "scaler-pg-test-1" {
			found = true
			if r.CapacityTotal != 10 || r.CapacityUsed != 3 || !r.Healthy {
				t.Fatalf("unexpected runner data: %+v", r)
			}
		}
	}
	if !found {
		t.Fatal("seeded runner not found in All() result")
	}
}

func TestPostgresRunnerSourceAllRespectsContextCancellation(t *testing.T) {
	source, _ := openTestPostgresSource(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before the call

	start := time.Now()
	_, err := source.All(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("All() with a pre-canceled context: error = nil, want a context-canceled error")
	}
	if elapsed > time.Second {
		t.Fatalf("All() with a pre-canceled context took %v, want a prompt return", elapsed)
	}
}
