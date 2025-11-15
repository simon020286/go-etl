package web

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	pipeline "github.com/simon020286/go-pipeline"
	"github.com/simon020286/go-pipeline/models"
	"github.com/simon020286/go-pipeline/pkg"
	_ "github.com/simon020286/go-pipeline/steps"
	"gopkg.in/yaml.v3"
)

// RunPipelineFile loads and runs a pipeline from a YAML file using the go-pipeline library
func RunPipelineFile(ctx context.Context, filePath string, logger *slog.Logger) error {
	// Load YAML file
	yamlData, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read pipeline file: %w", err)
	}

	// Parse YAML into config
	var config pkg.PipelineConfig
	if err := yaml.Unmarshal(yamlData, &config); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	logger.Info("Loaded pipeline", "name", config.Name, "description", config.Description, "stages", len(config.Stages))

	// Build pipeline from config
	p, err := pipeline.BuildFromConfig(&config)
	if err != nil {
		return fmt.Errorf("failed to build pipeline: %w", err)
	}

	// Add logger as event listener
	eventLogger := &pipelineEventLogger{logger: logger}
	p.AddListener(eventLogger)

	// Start pipeline
	startTime := time.Now()
	if err := p.Start(ctx); err != nil {
		return fmt.Errorf("failed to start pipeline: %w", err)
	}

	logger.Info("Pipeline is running...")

	// Wait for pipeline to complete or context cancellation
	p.Wait()

	elapsed := time.Since(startTime)
	logger.Info("Pipeline execution complete", "duration", elapsed)

	return nil
}

// pipelineEventLogger implements the EventListener interface to log pipeline events
type pipelineEventLogger struct {
	logger *slog.Logger
}

func (l *pipelineEventLogger) OnEvent(event models.Event) {
	timestamp := event.Timestamp.Format("15:04:05.000")

	switch event.Type {
	case models.EventPipelineStarted:
		mode := event.Data["mode"].(string)
		l.logger.Info("Pipeline started", "timestamp", timestamp, "mode", mode)

	case models.EventPipelineCompleted:
		duration := event.Data["duration"].(time.Duration)
		l.logger.Info("Pipeline completed", "timestamp", timestamp, "duration", duration)

	case models.EventPipelineError:
		err := event.Data["error"].(string)
		l.logger.Error("Pipeline error", "timestamp", timestamp, "error", err)

	case models.EventStageOutput:
		stageID := event.Data["stage_id"].(string)
		eventID := event.Data["event_id"].(string)
		l.logger.Debug("Stage produced output", "timestamp", timestamp, "stage_id", stageID, "event_id", eventID)

	case models.EventStageError:
		stageID := event.Data["stage_id"].(string)
		eventID := event.Data["event_id"].(string)
		err := event.Data["error"].(string)
		l.logger.Error("Stage error", "timestamp", timestamp, "stage_id", stageID, "event_id", eventID, "error", err)
	}
}
