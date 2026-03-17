package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/itkoren/loc-qrs/internal/observability"
	"github.com/itkoren/loc-qrs/internal/query"
	"github.com/itkoren/loc-qrs/internal/schema"
	mcpsyncer "github.com/itkoren/loc-qrs/internal/sync"
	"github.com/itkoren/loc-qrs/internal/writer"
)

// Deps groups all service dependencies for MCP tool handlers.
type Deps struct {
	FileWriter  *writer.FileWriter
	Encoder     writer.Encoder
	Schema      *schema.Schema
	SyncWorker  *mcpsyncer.SyncWorker
	QueryEngine *query.QueryEngine
	DataDir     string
	Metrics     *observability.Metrics
}

// RegisterTools registers all MCP tools onto the server.
func RegisterTools(s *server.MCPServer, deps Deps) {
	// write_record
	s.AddTool(mcp.NewTool("write_record",
		mcp.WithDescription("Ingest a record into the system"),
		mcp.WithObject("record",
			mcp.Required(),
			mcp.Description("The record fields as a JSON object"),
		),
	), writeRecordHandler(deps))

	// query_records
	s.AddTool(mcp.NewTool("query_records",
		mcp.WithDescription("Execute a DuckDB SQL query against ingested records. Reference 'records' as the table."),
		mcp.WithString("sql",
			mcp.Required(),
			mcp.Description("SQL query to execute (SELECT only). Use 'records' as table name."),
		),
	), queryRecordsHandler(deps))

	// get_schema
	s.AddTool(mcp.NewTool("get_schema",
		mcp.WithDescription("Return the current record schema definition"),
	), getSchemaHandler(deps))

	// list_files
	s.AddTool(mcp.NewTool("list_files",
		mcp.WithDescription("List all data files (JSONL, CSV, Parquet) in the data directory"),
	), listFilesHandler(deps))

	// sync_now
	s.AddTool(mcp.NewTool("sync_now",
		mcp.WithDescription("Trigger an immediate sync of JSONL/CSV files to Parquet"),
	), syncNowHandler(deps))

	// rebuild_index
	s.AddTool(mcp.NewTool("rebuild_index",
		mcp.WithDescription("Rebuild all Parquet files from source JSONL/CSV data"),
	), rebuildIndexHandler(deps))

	// get_health
	s.AddTool(mcp.NewTool("get_health",
		mcp.WithDescription("Return system health information"),
	), getHealthHandler(deps))
}

func writeRecordHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		recordRaw, ok := req.Params.Arguments["record"]
		if !ok {
			return mcp.NewToolResultError("missing 'record' argument"), nil
		}
		record, ok := recordRaw.(map[string]any)
		if !ok {
			return mcp.NewToolResultError("'record' must be a JSON object"), nil
		}

		if errs := deps.Schema.ValidateRecord(record); len(errs) > 0 {
			msg := "validation errors:"
			for _, e := range errs {
				msg += "\n  - " + e.Error()
			}
			return mcp.NewToolResultError(msg), nil
		}

		payload, err := deps.Encoder.Encode(record, deps.Schema)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("encode error: %v", err)), nil
		}

		rec := writer.Record{Payload: payload, IngestedAt: time.Now().UTC()}
		if !deps.FileWriter.Submit(rec) {
			return mcp.NewToolResultError("server busy: ingest channel full"), nil
		}

		if deps.Metrics != nil {
			deps.Metrics.RecordsIngested.Add(1)
		}

		return mcp.NewToolResultText("record accepted"), nil
	}
}

func queryRecordsHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		sqlStr, ok := req.Params.Arguments["sql"].(string)
		if !ok || sqlStr == "" {
			return mcp.NewToolResultError("missing or empty 'sql' argument"), nil
		}

		result, err := deps.QueryEngine.Execute(ctx, sqlStr)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("query error: %v", err)), nil
		}

		b, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
		}
		return mcp.NewToolResultText(string(b)), nil
	}
}

func getSchemaHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		out := map[string]any{
			"version": deps.Schema.Version,
			"format":  deps.Schema.Format,
			"columns": deps.Schema.Columns,
			"ordered": deps.Schema.ColumnNames(),
		}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
		}
		return mcp.NewToolResultText(string(b)), nil
	}
}

func listFilesHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		patterns := []string{"data_*.jsonl", "data_*.csv", "data_*.parquet"}
		var files []map[string]any

		for _, pat := range patterns {
			matches, err := filepath.Glob(filepath.Join(deps.DataDir, pat))
			if err != nil {
				continue
			}
			for _, m := range matches {
				info, err := os.Stat(m)
				if err != nil {
					continue
				}
				files = append(files, map[string]any{
					"path":       m,
					"size_bytes": info.Size(),
					"modified":   info.ModTime().UTC().Format(time.RFC3339),
				})
			}
		}

		if files == nil {
			files = []map[string]any{}
		}
		b, err := json.MarshalIndent(files, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
		}
		return mcp.NewToolResultText(string(b)), nil
	}
}

func syncNowHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := deps.SyncWorker.SyncNow(ctx); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("sync error: %v", err)), nil
		}
		return mcp.NewToolResultText("sync completed"), nil
	}
}

func rebuildIndexHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := deps.SyncWorker.RebuildAll(ctx); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("rebuild error: %v", err)), nil
		}
		return mcp.NewToolResultText("rebuild completed"), nil
	}
}

func getHealthHandler(deps Deps) server.ToolHandlerFunc {
	return func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		fillPct := 0.0
		if deps.FileWriter != nil && deps.FileWriter.ChannelCap() > 0 {
			fillPct = float64(deps.FileWriter.ChannelLen()) / float64(deps.FileWriter.ChannelCap()) * 100
		}
		out := map[string]any{
			"status":           "ok",
			"channel_fill_pct": fillPct,
		}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
		}
		return mcp.NewToolResultText(string(b)), nil
	}
}
