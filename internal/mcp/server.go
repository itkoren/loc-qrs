package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/mark3labs/mcp-go/server"
)

// Server wraps the mcp-go server with both stdio and SSE transport support.
type Server struct {
	mcp    *server.MCPServer
	sse    *server.SSEServer
	logger *slog.Logger
	addr   string
}

// NewServer creates a new MCP server instance.
func NewServer(deps Deps, addr string, logger *slog.Logger) *Server {
	s := server.NewMCPServer(
		"loc-qrs",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	RegisterTools(s, deps)

	return &Server{
		mcp:    s,
		logger: logger,
		addr:   addr,
	}
}

// ServeStdio runs the MCP server over stdin/stdout (for Claude Desktop and local agents).
func (s *Server) ServeStdio(ctx context.Context) error {
	s.logger.Info("starting MCP stdio server")
	return server.NewStdioServer(s.mcp).Listen(ctx, nil, nil)
}

// Start launches the MCP HTTP/SSE server in the background.
func (s *Server) Start(ctx context.Context) error {
	s.sse = server.NewSSEServer(s.mcp,
		server.WithBaseURL(fmt.Sprintf("http://%s", s.addr)),
	)
	s.logger.Info("starting MCP SSE server", "addr", s.addr)
	go func() {
		if err := s.sse.Start(s.addr); err != nil && err != http.ErrServerClosed {
			s.logger.Error("MCP SSE server error", "error", err)
		}
	}()
	return nil
}

// Shutdown gracefully stops the MCP HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.sse != nil {
		return s.sse.Shutdown(ctx)
	}
	return nil
}
