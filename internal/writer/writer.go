package writer

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/schema"
)

// Record is what flows through the ingest channel.
type Record struct {
	Payload    []byte    // pre-encoded line (includes \n)
	IngestedAt time.Time
}

// FileWriter manages the single write goroutine, buffered file I/O, and roll events.
type FileWriter struct {
	ch              chan Record
	syncTriggerCh   chan<- struct{}
	rollCh          chan<- RollEvent
	rotator         *DailyRotator
	encoder         Encoder
	schema          *schema.Schema
	syncRecordCount int
	metrics         *observability.Metrics
	logger          *slog.Logger

	// current file state — only accessed from the run() goroutine (no mutex needed)
	file *os.File
	bw   *bufio.Writer

	counter atomic.Int64

	// flushCh carries flush requests from external goroutines into run().
	// Sending a chan error to flushCh causes run() to drain ch, flush bw, and
	// send the result back. This guarantees all records submitted before the
	// Flush() call are written to disk before Flush() returns.
	flushCh chan chan error

	doneCh chan struct{}

	// mu guards stopOnce so Stop() is idempotent
	mu       sync.Mutex
	stopOnce bool
}

// NewFileWriter creates a FileWriter. Call Start() to begin processing.
func NewFileWriter(
	ch chan Record,
	syncTriggerCh chan<- struct{},
	rollCh chan<- RollEvent,
	rotator *DailyRotator,
	encoder Encoder,
	sch *schema.Schema,
	syncRecordCount int,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *FileWriter {
	return &FileWriter{
		ch:              ch,
		syncTriggerCh:   syncTriggerCh,
		rollCh:          rollCh,
		rotator:         rotator,
		encoder:         encoder,
		schema:          sch,
		syncRecordCount: syncRecordCount,
		metrics:         metrics,
		logger:          logger,
		flushCh:         make(chan chan error),
		doneCh:          make(chan struct{}),
	}
}

// Submit attempts to enqueue a record. Returns false if the channel is full.
func (fw *FileWriter) Submit(r Record) bool {
	select {
	case fw.ch <- r:
		return true
	default:
		return false
	}
}

// ChannelLen returns the number of records currently buffered.
func (fw *FileWriter) ChannelLen() int {
	return len(fw.ch)
}

// ChannelCap returns the channel capacity.
func (fw *FileWriter) ChannelCap() int {
	return cap(fw.ch)
}

// Flush ensures all records submitted before this call are written to disk.
// It passes a flush request through flushCh so that run() processes it after
// all currently-queued records, guaranteeing correct ordering.
// If the writer goroutine has already stopped, Flush returns nil immediately.
func (fw *FileWriter) Flush() error {
	doneCh := make(chan error, 1)
	select {
	case fw.flushCh <- doneCh:
		return <-doneCh
	case <-fw.doneCh:
		// Writer goroutine already stopped; final flush was done in Stop().
		return nil
	}
}

// Start launches the write goroutine.
func (fw *FileWriter) Start() {
	go fw.run()
}

// Stop closes the channel, waits for the write goroutine to drain it, and flushes.
func (fw *FileWriter) Stop() {
	fw.mu.Lock()
	if fw.stopOnce {
		fw.mu.Unlock()
		return
	}
	fw.stopOnce = true
	fw.mu.Unlock()

	close(fw.ch)
	<-fw.doneCh
}

// CurrentFilePath returns the path of the currently active data file.
func (fw *FileWriter) CurrentFilePath() string {
	today := CurrentDate()
	return fw.rotator.FilePath(today)
}

// run is the single write goroutine. It processes records and flush requests.
func (fw *FileWriter) run() {
	defer close(fw.doneCh)

	for {
		select {
		case r, ok := <-fw.ch:
			if !ok {
				// Channel closed by Stop(): perform final flush and exit.
				fw.finalFlush()
				return
			}
			if err := fw.writeRecord(r); err != nil {
				fw.logger.Error("write record failed", "error", err)
			}
			if fw.metrics != nil {
				fw.metrics.IngestionChannelDepth.Set(float64(len(fw.ch)))
			}

		case respCh := <-fw.flushCh:
			// Drain any records that arrived before this flush request.
			// Use a non-blocking inner loop so we don't block indefinitely.
			for {
				select {
				case r, ok := <-fw.ch:
					if !ok {
						// Channel closed during drain; final flush and signal.
						fw.finalFlush()
						respCh <- nil
						return
					}
					if err := fw.writeRecord(r); err != nil {
						fw.logger.Error("write record failed during flush drain", "error", err)
					}
				default:
					goto drained
				}
			}
		drained:
			// All queued records written; now flush bufio to disk.
			var err error
			if fw.bw != nil {
				err = fw.bw.Flush()
			}
			respCh <- err
		}
	}
}

// finalFlush flushes and closes the current file. Called only from run() during shutdown.
func (fw *FileWriter) finalFlush() {
	if fw.bw != nil {
		if err := fw.bw.Flush(); err != nil {
			fw.logger.Error("final flush failed", "error", err)
		}
	}
	if fw.file != nil {
		if err := fw.file.Close(); err != nil {
			fw.logger.Error("close file on shutdown", "error", err)
		}
		fw.file = nil
		fw.bw = nil
	}
}

// writeRecord handles rotation, opens/creates the file as needed, and writes the payload.
// Must only be called from run().
func (fw *FileWriter) writeRecord(r Record) error {
	path, roll := fw.rotator.Check()

	if roll != nil {
		// Flush and close old file.
		if fw.bw != nil {
			if err := fw.bw.Flush(); err != nil {
				fw.logger.Error("flush on roll", "error", err)
			}
		}
		if fw.file != nil {
			if err := fw.file.Close(); err != nil {
				fw.logger.Error("close on roll", "error", err)
			}
			fw.file = nil
			fw.bw = nil
		}
		// Notify sync worker.
		select {
		case fw.rollCh <- *roll:
		default:
			fw.logger.Warn("rollCh full, dropping roll event", "path", roll.OldPath)
		}
	}

	if fw.file == nil {
		if err := fw.openFile(path); err != nil {
			return fmt.Errorf("open data file %s: %w", path, err)
		}
	}

	if _, err := fw.bw.Write(r.Payload); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	n := fw.counter.Add(1)
	if fw.syncRecordCount > 0 && int(n)%fw.syncRecordCount == 0 {
		select {
		case fw.syncTriggerCh <- struct{}{}:
		default:
		}
	}

	return nil
}

// openFile opens or creates the data file for appending. Must only be called from run().
func (fw *FileWriter) openFile(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fw.file = f
	fw.bw = bufio.NewWriterSize(f, 64*1024)
	return nil
}
