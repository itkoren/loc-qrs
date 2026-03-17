package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/itkoren/loc-qrs/internal/api"
	"github.com/itkoren/loc-qrs/internal/config"
	mcpserver "github.com/itkoren/loc-qrs/internal/mcp"
	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/query"
	"github.com/itkoren/loc-qrs/internal/schema"
	locSync "github.com/itkoren/loc-qrs/internal/sync"
	"github.com/itkoren/loc-qrs/internal/writer"
)

func main() {
	cfg := config.Load()
	logger := observability.SetupLogger(os.Getenv("LOG_LEVEL"))

	if err := run(cfg, logger); err != nil {
		logger.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg *config.Config, logger *slog.Logger) error {
	// --- Schema ---
	sch, err := schema.Load(cfg.SchemaPath)
	if err != nil {
		return fmt.Errorf("load schema: %w", err)
	}
	logger.Info("schema loaded", "version", sch.Version[:8], "columns", len(sch.Columns))

	// Schema version check.
	if err := checkSchemaVersion(cfg.DataDir, sch.Version, cfg.Rebuild); err != nil {
		return err
	}

	// Ensure data directory exists.
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	// --- Metrics ---
	metrics := observability.NewMetrics(nil)

	// --- Encoder ---
	enc, err := writer.NewEncoder(string(cfg.Format))
	if err != nil {
		return fmt.Errorf("create encoder: %w", err)
	}

	// --- Channels ---
	ch := make(chan writer.Record, cfg.ChannelCapacity)
	syncTriggerCh := make(chan struct{}, 1)
	rollCh := make(chan writer.RollEvent, 4)

	// --- File Writer ---
	ext := string(cfg.Format)
	if ext == "" {
		ext = "jsonl"
	}
	rotator := writer.NewDailyRotator(cfg.DataDir, ext)
	fw := writer.NewFileWriter(
		ch,
		syncTriggerCh,
		rollCh,
		rotator,
		enc,
		sch,
		cfg.SyncRecordCount,
		metrics,
		logger,
	)

	// --- DuckDB instances ---
	syncDBPath := filepath.Join(cfg.DataDir, ".sync.duckdb")
	syncDB, err := locSync.OpenSyncDB(syncDBPath)
	if err != nil {
		return fmt.Errorf("open sync duckdb: %w", err)
	}

	queryDB, err := locSync.OpenQueryDB()
	if err != nil {
		syncDB.Close()
		return fmt.Errorf("open query duckdb: %w", err)
	}

	// --- Sync Worker ---
	sw := locSync.NewSyncWorker(
		syncDB,
		fw,
		cfg.DataDir,
		string(cfg.Format),
		cfg.SyncInterval,
		syncTriggerCh,
		rollCh,
		metrics,
		logger,
	)

	// Rebuild on startup if requested.
	if cfg.Rebuild {
		logger.Info("rebuilding all parquet files")
		if err := sw.RebuildAll(context.Background()); err != nil {
			return fmt.Errorf("rebuild: %w", err)
		}
	}

	// --- Query Engine ---
	qe := query.NewQueryEngine(queryDB, cfg.DataDir, fw, metrics, logger)

	// --- HTTP Server ---
	router := api.NewRouter(api.ServerDeps{
		FileWriter:  fw,
		Encoder:     enc,
		Schema:      sch,
		SyncWorker:  sw,
		QueryEngine: qe,
		QueryDB:     queryDB,
		Metrics:     metrics,
		Logger:      logger,
	})

	httpSrv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// --- MCP Server ---
	mcpDeps := mcpserver.Deps{
		FileWriter:  fw,
		Encoder:     enc,
		Schema:      sch,
		SyncWorker:  sw,
		QueryEngine: qe,
		DataDir:     cfg.DataDir,
		Metrics:     metrics,
	}
	mcpSrv := mcpserver.NewServer(mcpDeps, cfg.MCPAddr, logger)

	// --- Signal handling ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// --- Start background workers ---
	fw.Start()
	sw.Start()

	if cfg.MCPStdio {
		// Stdio mode: block on MCP stdio, then shut down.
		logger.Info("running in MCP stdio mode")
		if err := mcpSrv.ServeStdio(ctx); err != nil {
			logger.Error("stdio MCP error", "error", err)
		}
		return orderedShutdown(ctx, cfg, httpSrv, fw, sw, mcpSrv, syncDB, queryDB, logger)
	}

	// Start MCP SSE server.
	if err := mcpSrv.Start(ctx); err != nil {
		return fmt.Errorf("start MCP server: %w", err)
	}

	// Start HTTP server.
	httpErrCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			httpErrCh <- fmt.Errorf("HTTP server: %w", err)
		}
		close(httpErrCh)
	}()

	// Wait for shutdown signal or HTTP server failure.
	select {
	case sig := <-sigCh:
		logger.Info("received signal", "signal", sig)
		cancel()
	case err := <-httpErrCh:
		if err != nil {
			logger.Error("HTTP server failed", "error", err)
		}
		cancel()
	case <-ctx.Done():
	}

	return orderedShutdown(ctx, cfg, httpSrv, fw, sw, mcpSrv, syncDB, queryDB, logger)
}

// orderedShutdown performs the graceful shutdown sequence.
func orderedShutdown(
	ctx context.Context,
	cfg *config.Config,
	httpSrv *http.Server,
	fw *writer.FileWriter,
	sw *locSync.SyncWorker,
	mcpSrv *mcpserver.Server,
	syncDB interface{ Close() error },
	queryDB interface{ Close() error },
	logger *slog.Logger,
) error {
	shutCtx, shutCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutCancel()

	// 1. Stop accepting HTTP requests.
	logger.Info("shutting down HTTP server")
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		logger.Warn("HTTP shutdown error", "error", err)
	}

	// 2. Drain write channel + flush.
	logger.Info("stopping file writer")
	fw.Stop()

	// 3. Final sync.
	logger.Info("running final sync")
	if err := sw.SyncNow(shutCtx); err != nil {
		logger.Warn("final sync error", "error", err)
	}
	sw.Stop()

	// 4. Close DuckDB instances.
	logger.Info("closing databases")
	if err := syncDB.Close(); err != nil {
		logger.Warn("close syncDB error", "error", err)
	}
	if err := queryDB.Close(); err != nil {
		logger.Warn("close queryDB error", "error", err)
	}

	// 5. Shutdown MCP server.
	logger.Info("shutting down MCP server")
	if err := mcpSrv.Shutdown(shutCtx); err != nil {
		logger.Warn("MCP shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
	return nil
}

// checkSchemaVersion reads or writes the schema version file.
// If the version has changed and --rebuild was not passed, it returns an error.
func checkSchemaVersion(dataDir, version string, rebuild bool) error {
	versionFile := filepath.Join(dataDir, ".schema_version")

	// Ensure data directory exists for version file.
	_ = os.MkdirAll(dataDir, 0o755)

	stored, err := os.ReadFile(versionFile)
	if err != nil {
		if os.IsNotExist(err) {
			// First run: write version.
			return os.WriteFile(versionFile, []byte(version), 0o644)
		}
		return fmt.Errorf("read schema version: %w", err)
	}

	storedVersion := string(stored)
	if storedVersion == version {
		return nil
	}

	if !rebuild {
		return fmt.Errorf(
			"schema version mismatch (stored=%s, current=%s): run with --rebuild to re-sync all data",
			storedVersion[:8], version[:8],
		)
	}

	// Update version after rebuild.
	_ = os.WriteFile(versionFile, []byte(version), 0o644)
	return nil
}

// suppress unused import
var _ = sha256.Sum256
