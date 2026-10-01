// Command scaler runs the standalone runner-VMSS capacity scaler: it reads
// runner capacity directly from the shared Postgres runners table, evaluates
// the scaling policy on a ticker, and acts on the target Azure VMSS.
package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver

	"github.com/n8n-io/sandbox-service/internal/azurescale"
	"github.com/n8n-io/sandbox-service/internal/metrics"
	"github.com/n8n-io/sandbox-service/internal/obs"
	"github.com/n8n-io/sandbox-service/internal/scaler"
	"github.com/n8n-io/sandbox-service/internal/scaler/config"
)

func main() {
	var logLevel slog.LevelVar
	logLevel.Set(slog.LevelInfo)
	logger := slog.New(obs.TraceHandler(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &logLevel})))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load scaler config", "error", err)
		os.Exit(1)
	}
	logLevel.Set(cfg.LogLevel)

	db, err := sql.Open("pgx", cfg.Postgres.DSN())
	if err != nil {
		slog.Error("open postgres connection", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	source := scaler.NewPostgresRunnerSource(db)

	vmss, err := azurescale.New(azurescale.Target{
		SubscriptionID:           cfg.Policy.AzureSubscriptionID,
		ResourceGroup:            cfg.Policy.AzureResourceGroup,
		VMSSName:                 cfg.Policy.AzureVMSSName,
		WorkloadIdentityClientID: cfg.AzureClientID,
		TenantID:                 cfg.AzureTenantID,
	})
	if err != nil {
		slog.Error("failed to create vmss scaler client", "error", err)
		os.Exit(1)
	}

	// Metrics are always enabled for this binary: unlike the API (which has a
	// tenant-facing reason to gate /metrics), this process has no public
	// surface at all, so there is no cost/risk to always exposing it.
	rec := metrics.NewScalerRecorder(true)

	s := scaler.New(cfg.Policy, cfg.HeartbeatGrace, source, vmss, rec)
	scaler.LogConfig(cfg.Policy)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           scaler.NewServer(s, cfg.APIToken),
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("scaler backchannel listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-quit:
		slog.Info("received signal, shutting down scaler", "signal", sig)
		cancel()
	case err := <-serverErr:
		cancel()
		slog.Error("server error", "error", err)
		os.Exit(1)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		slog.Error("graceful shutdown failed", "error", err)
	}

	slog.Info("scaler stopped")
}
