package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"go-etl/core"
	"go-etl/pipeline"
	_ "go-etl/steps"
	"go-etl/web"
)

func main() {
	webFlag := flag.Bool("web", false, "Start web server")
	logFlag := flag.String("log", "debug", "Set log level (debug, info, warn, error)")
	fileFlag := flag.String("file", "", "Path to pipeline YAML file")

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
		Level: logLevel, // o slog.LevelInfo
	}))

	slog.SetDefault(logger)

	if fileFlag == nil && !*webFlag {
		logger.Error("No pipeline file specified. Use -file to provide a YAML file or -web to start the web server.")
		return
	}

	defer core.StopWebServer(context.Background())

	if *webFlag {
		web.StartServer(logger)
		return
	}

	pipeline, err := pipeline.LoadPipelineFromFile(*fileFlag)
	if err != nil {
		logger.Error("Failed to load pipeline", "error", err)
		return
	}

	ctx := context.Background()
	if err = pipeline.Run(ctx, logger); err != nil {
		logger.Error("Pipeline run failed", "error", err)
		return
	}
}
