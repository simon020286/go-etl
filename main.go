package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go-etl/web"
)

func main() {
	webFlag := flag.Bool("web", false, "Start web server")
	logFlag := flag.String("log", "warn", "Set log level (debug, info, warn, error)")
	fileFlag := flag.String("file", "", "Path to pipeline YAML file (runs once and exits)")

	flag.Parse()

	logLevel := slog.LevelDebug
	switch *logFlag {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo // Default to info if invalid level
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	slog.SetDefault(logger)

	if !*webFlag && *fileFlag == "" {
		logger.Error("No mode specified. Use -web to start the web server or -file to run a pipeline")
		os.Exit(1)
	}

	// Create context that cancels on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal, shutting down gracefully", "signal", sig)
		cancel()
	}()

	if *webFlag {
		// Start web server mode - this is the main orchestration mode
		logger.Info("Starting go-etl orchestrator in web mode")
		web.StartServer(logger)
		return
	}

	if *fileFlag != "" {
		// Direct pipeline execution mode - runs a single pipeline file and exits
		logger.Info("Running pipeline from file", "file", *fileFlag)
		if err := web.RunPipelineFile(ctx, *fileFlag, logger); err != nil {
			logger.Error("Pipeline execution failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Pipeline execution completed successfully")
	}
}
