// Package config loads the standalone scaler's configuration from
// SANDBOX_SCALER_* environment variables (defaults and semantics:
// docs/configuration.md).
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/n8n-io/sandbox-service/internal/logging"
	"github.com/n8n-io/sandbox-service/internal/scaler"
)

const (
	defaultScalerCooldown   = 5 * time.Minute
	defaultScalerSustained  = 10 * time.Minute
	defaultScalerEvalPeriod = time.Minute
	defaultListenAddr       = ":8090"
	defaultLogLevel         = slog.LevelInfo
	defaultPostgresPort     = 5432
	defaultPostgresSSLMode  = "require"
)

// PostgresConfig holds connection settings for the scaler's own Postgres
// connection to the shared runners table.
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// DSN returns a libpq connection string for pgx/stdlib.
func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, p.SSLMode,
	)
}

// Config is the standalone scaler's full configuration.
type Config struct {
	Policy scaler.Policy

	// HeartbeatGrace is how fresh a runner's last heartbeat must be to count
	// toward the capacity signal (same semantics as the API's
	// SANDBOX_API_RUNNER_HEARTBEAT_GRACE).
	HeartbeatGrace time.Duration

	// AzureClientID and AzureTenantID select the workload-identity federated
	// credential. Empty falls back to the ambient AZURE_CLIENT_ID /
	// AZURE_TENANT_ID environment variables set by the Azure workload
	// identity webhook.
	AzureClientID string
	AzureTenantID string

	Postgres PostgresConfig

	// ListenAddr is where the internal GET /policy backchannel is served.
	ListenAddr string
	// APIToken is the shared bearer token the tenant-facing API must present.
	APIToken string

	LogLevel slog.Level
}

// Load reads the scaler configuration from SANDBOX_SCALER_* environment
// variables.
func Load() (*Config, error) {
	cfg := &Config{
		HeartbeatGrace: 45 * time.Second,
		ListenAddr:     defaultListenAddr,
		LogLevel:       defaultLogLevel,
		Postgres:       PostgresConfig{Port: defaultPostgresPort, SSLMode: defaultPostgresSSLMode},
		Policy: scaler.Policy{
			ScaleInSustainedFor: defaultScalerSustained,
			Cooldown:            defaultScalerCooldown,
			EvalInterval:        defaultScalerEvalPeriod,
		},
	}

	if v := os.Getenv("SANDBOX_SCALER_LOG_LEVEL"); v != "" {
		lvl, err := logging.ParseLevel(v)
		if err != nil {
			return nil, fmt.Errorf("SANDBOX_SCALER_LOG_LEVEL: %w", err)
		}
		cfg.LogLevel = lvl
	}

	required := map[string]string{
		"SANDBOX_SCALER_MIN_NODES":             os.Getenv("SANDBOX_SCALER_MIN_NODES"),
		"SANDBOX_SCALER_MAX_NODES":             os.Getenv("SANDBOX_SCALER_MAX_NODES"),
		"SANDBOX_SCALER_SCALE_OUT_THRESHOLD":   os.Getenv("SANDBOX_SCALER_SCALE_OUT_THRESHOLD"),
		"SANDBOX_SCALER_SCALE_IN_THRESHOLD":    os.Getenv("SANDBOX_SCALER_SCALE_IN_THRESHOLD"),
		"SANDBOX_SCALER_AZURE_SUBSCRIPTION_ID": os.Getenv("SANDBOX_SCALER_AZURE_SUBSCRIPTION_ID"),
		"SANDBOX_SCALER_AZURE_RESOURCE_GROUP":  os.Getenv("SANDBOX_SCALER_AZURE_RESOURCE_GROUP"),
		"SANDBOX_SCALER_AZURE_VMSS_NAME":       os.Getenv("SANDBOX_SCALER_AZURE_VMSS_NAME"),
		"SANDBOX_SCALER_POSTGRES_HOST":         os.Getenv("SANDBOX_SCALER_POSTGRES_HOST"),
		"SANDBOX_SCALER_POSTGRES_USER":         os.Getenv("SANDBOX_SCALER_POSTGRES_USER"),
		"SANDBOX_SCALER_POSTGRES_PASSWORD":     os.Getenv("SANDBOX_SCALER_POSTGRES_PASSWORD"),
		"SANDBOX_SCALER_POSTGRES_DB":           os.Getenv("SANDBOX_SCALER_POSTGRES_DB"),
		"SANDBOX_SCALER_API_TOKEN":             os.Getenv("SANDBOX_SCALER_API_TOKEN"),
	}
	for name, v := range required {
		if strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("%s must be set", name)
		}
	}

	minNodes, err := strconv.Atoi(strings.TrimSpace(required["SANDBOX_SCALER_MIN_NODES"]))
	if err != nil || minNodes < 0 {
		return nil, fmt.Errorf("SANDBOX_SCALER_MIN_NODES must be a non-negative integer, got %q", required["SANDBOX_SCALER_MIN_NODES"])
	}
	maxNodes, err := strconv.Atoi(strings.TrimSpace(required["SANDBOX_SCALER_MAX_NODES"]))
	if err != nil || maxNodes < minNodes {
		return nil, fmt.Errorf("SANDBOX_SCALER_MAX_NODES must be an integer >= SANDBOX_SCALER_MIN_NODES (%d), got %q", minNodes, required["SANDBOX_SCALER_MAX_NODES"])
	}
	scaleOut, err := strconv.Atoi(strings.TrimSpace(required["SANDBOX_SCALER_SCALE_OUT_THRESHOLD"]))
	if err != nil || scaleOut < 0 {
		return nil, fmt.Errorf("SANDBOX_SCALER_SCALE_OUT_THRESHOLD must be a non-negative integer, got %q", required["SANDBOX_SCALER_SCALE_OUT_THRESHOLD"])
	}
	scaleIn, err := strconv.Atoi(strings.TrimSpace(required["SANDBOX_SCALER_SCALE_IN_THRESHOLD"]))
	if err != nil || scaleIn < 0 {
		return nil, fmt.Errorf("SANDBOX_SCALER_SCALE_IN_THRESHOLD must be a non-negative integer, got %q", required["SANDBOX_SCALER_SCALE_IN_THRESHOLD"])
	}

	cfg.Policy.MinNodes = minNodes
	cfg.Policy.MaxNodes = maxNodes
	cfg.Policy.ScaleOutThreshold = scaleOut
	cfg.Policy.ScaleInThreshold = scaleIn
	cfg.Policy.AzureSubscriptionID = strings.TrimSpace(required["SANDBOX_SCALER_AZURE_SUBSCRIPTION_ID"])
	cfg.Policy.AzureResourceGroup = strings.TrimSpace(required["SANDBOX_SCALER_AZURE_RESOURCE_GROUP"])
	cfg.Policy.AzureVMSSName = strings.TrimSpace(required["SANDBOX_SCALER_AZURE_VMSS_NAME"])

	cfg.AzureClientID = strings.TrimSpace(os.Getenv("SANDBOX_SCALER_AZURE_CLIENT_ID"))
	cfg.AzureTenantID = strings.TrimSpace(os.Getenv("SANDBOX_SCALER_AZURE_TENANT_ID"))

	cfg.Postgres.Host = strings.TrimSpace(required["SANDBOX_SCALER_POSTGRES_HOST"])
	cfg.Postgres.User = strings.TrimSpace(required["SANDBOX_SCALER_POSTGRES_USER"])
	cfg.Postgres.Password = required["SANDBOX_SCALER_POSTGRES_PASSWORD"]
	cfg.Postgres.Database = strings.TrimSpace(required["SANDBOX_SCALER_POSTGRES_DB"])
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_POSTGRES_PORT")); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil || port <= 0 {
			return nil, fmt.Errorf("SANDBOX_SCALER_POSTGRES_PORT must be a positive integer, got %q", v)
		}
		cfg.Postgres.Port = port
	}
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_POSTGRES_SSLMODE")); v != "" {
		cfg.Postgres.SSLMode = v
	}

	cfg.APIToken = required["SANDBOX_SCALER_API_TOKEN"]

	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_LISTEN_ADDR")); v != "" {
		cfg.ListenAddr = v
	}
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_RUNNER_HEARTBEAT_GRACE")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("SANDBOX_SCALER_RUNNER_HEARTBEAT_GRACE must be a positive duration, got %q", v)
		}
		cfg.HeartbeatGrace = d
	}
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_SCALE_IN_SUSTAINED_FOR")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			return nil, fmt.Errorf("SANDBOX_SCALER_SCALE_IN_SUSTAINED_FOR must be a non-negative duration, got %q", v)
		}
		cfg.Policy.ScaleInSustainedFor = d
	}
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_COOLDOWN")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("SANDBOX_SCALER_COOLDOWN must be a positive duration, got %q", v)
		}
		cfg.Policy.Cooldown = d
	}
	if v := strings.TrimSpace(os.Getenv("SANDBOX_SCALER_EVAL_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("SANDBOX_SCALER_EVAL_INTERVAL must be a positive duration, got %q", v)
		}
		cfg.Policy.EvalInterval = d
	}

	return cfg, nil
}
