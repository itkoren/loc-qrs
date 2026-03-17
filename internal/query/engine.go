package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/writer"
)

// QueryResult is the result of a query execution.
type QueryResult struct {
	Columns []string `json:"columns"`
	Rows    [][]any  `json:"rows"`
}

// QueryEngine executes read-only queries against Parquet + live JSONL files.
type QueryEngine struct {
	db         *sql.DB
	dataDir    string
	fileWriter interface{ CurrentFilePath() string }
	metrics    *observability.Metrics
	logger     *slog.Logger
}

// NewQueryEngine creates a QueryEngine.
func NewQueryEngine(
	db *sql.DB,
	dataDir string,
	fw *writer.FileWriter,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *QueryEngine {
	// Guard against typed-nil interface: only assign fw when non-nil.
	var fwIface interface{ CurrentFilePath() string }
	if fw != nil {
		fwIface = fw
	}
	return &QueryEngine{
		db:         db,
		dataDir:    dataDir,
		fileWriter: fwIface,
		metrics:    metrics,
		logger:     logger,
	}
}

// Execute validates and runs the user SQL, returning structured results.
func (qe *QueryEngine) Execute(ctx context.Context, userSQL string) (*QueryResult, error) {
	start := time.Now()

	if err := GuardSQL(userSQL); err != nil {
		if qe.metrics != nil {
			qe.metrics.QueryErrors.WithLabelValues("forbidden").Add(1)
		}
		return nil, fmt.Errorf("query guard: %w", err)
	}

	currentFile := ""
	if qe.fileWriter != nil {
		currentFile = qe.fileWriter.CurrentFilePath()
	}

	fullSQL, err := BuildQuery(userSQL, qe.dataDir, currentFile)
	if err != nil {
		if qe.metrics != nil {
			qe.metrics.QueryErrors.WithLabelValues("build").Add(1)
		}
		return nil, fmt.Errorf("build query: %w", err)
	}

	qe.logger.Debug("executing query", "sql", fullSQL)

	rows, err := qe.db.QueryContext(ctx, fullSQL)
	if err != nil {
		if qe.metrics != nil {
			qe.metrics.QueryErrors.WithLabelValues("execute").Add(1)
		}
		return nil, fmt.Errorf("query execute: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	result := &QueryResult{Columns: cols}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		result.Rows = append(result.Rows, vals)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	elapsed := time.Since(start).Seconds()
	if qe.metrics != nil {
		qe.metrics.QueryLatency.Observe(elapsed)
	}

	return result, nil
}
