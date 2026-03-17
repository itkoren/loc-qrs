package sync

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/writer"
)

// Flusher is implemented by FileWriter to flush buffered writes before syncing.
type Flusher interface {
	Flush() error
}

// SyncWorker manages periodic and on-demand syncing of data files to Parquet.
type SyncWorker struct {
	db      *sql.DB
	flusher Flusher
	dataDir string
	format  string // "jsonl" or "csv"
	metrics *observability.Metrics
	logger  *slog.Logger

	triggerCh <-chan struct{}
	rollCh    <-chan writer.RollEvent
	ticker    *time.Ticker
	interval  time.Duration

	mu      sync.Mutex
	running bool

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewSyncWorker creates a SyncWorker.
func NewSyncWorker(
	db *sql.DB,
	flusher Flusher,
	dataDir string,
	format string,
	interval time.Duration,
	triggerCh <-chan struct{},
	rollCh <-chan writer.RollEvent,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *SyncWorker {
	return &SyncWorker{
		db:        db,
		flusher:   flusher,
		dataDir:   dataDir,
		format:    format,
		metrics:   metrics,
		logger:    logger,
		triggerCh: triggerCh,
		rollCh:    rollCh,
		interval:  interval,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Start begins the sync worker background loop.
func (sw *SyncWorker) Start() {
	sw.ticker = time.NewTicker(sw.interval)
	go sw.run()
}

// Stop signals the worker to stop and waits for it to exit.
func (sw *SyncWorker) Stop() {
	sw.ticker.Stop()
	close(sw.stopCh)
	<-sw.doneCh
}

// SyncNow triggers an immediate sync and blocks until it completes.
func (sw *SyncWorker) SyncNow(ctx context.Context) error {
	return sw.syncAll(ctx)
}

// RebuildAll re-syncs every source file (JSONL or CSV) in the data directory to Parquet.
func (sw *SyncWorker) RebuildAll(ctx context.Context) error {
	sw.logger.Info("rebuilding all parquet files")
	return sw.syncAll(ctx)
}

// run is the background loop.
func (sw *SyncWorker) run() {
	defer close(sw.doneCh)
	ctx := context.Background()

	for {
		select {
		case <-sw.stopCh:
			return
		case <-sw.ticker.C:
			if err := sw.syncAll(ctx); err != nil {
				sw.logger.Error("periodic sync failed", "error", err)
			}
		case <-sw.triggerCh:
			if err := sw.syncAll(ctx); err != nil {
				sw.logger.Error("triggered sync failed", "error", err)
			}
		case ev, ok := <-sw.rollCh:
			if !ok {
				return
			}
			if err := sw.syncFile(ctx, ev.OldPath); err != nil {
				sw.logger.Error("roll sync failed", "path", ev.OldPath, "error", err)
			}
		}
	}
}

// syncAll flushes the writer and syncs all source files.
func (sw *SyncWorker) syncAll(ctx context.Context) error {
	sw.mu.Lock()
	if sw.running {
		sw.mu.Unlock()
		return nil // coalesce concurrent triggers
	}
	sw.running = true
	sw.mu.Unlock()
	defer func() {
		sw.mu.Lock()
		sw.running = false
		sw.mu.Unlock()
	}()

	if sw.metrics != nil {
		sw.metrics.ActiveSyncs.Inc()
		defer sw.metrics.ActiveSyncs.Dec()
	}

	// Flush buffered writes first.
	if err := sw.flusher.Flush(); err != nil {
		sw.logger.Warn("flush before sync failed", "error", err)
	}

	ext := sw.format
	if ext == "" {
		ext = "jsonl"
	}
	pattern := filepath.Join(sw.dataDir, "data_*."+ext)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob source files: %w", err)
	}

	start := time.Now()
	var syncErr error
	for _, f := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := sw.syncFile(ctx, f); err != nil {
			sw.logger.Error("sync file failed", "file", f, "error", err)
			syncErr = err
			if sw.metrics != nil {
				sw.metrics.SyncFailures.Add(1)
			}
		}
	}

	elapsed := time.Since(start).Seconds()
	if sw.metrics != nil {
		sw.metrics.ParquetSyncDuration.Observe(elapsed)
	}
	sw.logger.Info("sync complete", "files", len(files), "duration_s", elapsed)
	return syncErr
}

// syncFile converts a single source file to Parquet.
func (sw *SyncWorker) syncFile(_ context.Context, srcPath string) error {
	// Skip empty files.
	info, err := os.Stat(srcPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() == 0 {
		return nil
	}

	dstPath := ParquetPathForSource(srcPath)

	ext := strings.ToLower(filepath.Ext(srcPath))
	switch ext {
	case ".jsonl":
		return CopyToParquet(sw.db, srcPath, dstPath)
	case ".csv":
		return CopyCSVToParquet(sw.db, srcPath, dstPath, false)
	default:
		return fmt.Errorf("unsupported source extension %q", ext)
	}
}
