package steps

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go-etl/core"
	"go-etl/pipeline"
)

type CronStep struct {
	name     string
	schedule string
	ticker   *time.Ticker
	stopChan chan struct{}
}

func (s *CronStep) Name() string { return s.name }

func (s *CronStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// This method is not used for cron triggers
	return core.CreateDefaultResultData("Cron triggered"), nil
}

func (s *CronStep) SetOnTrigger(callback func(data map[string]*core.Data)) error {
	// Parse the schedule and start the ticker
	duration, err := parseCronExpression(s.schedule)
	if err != nil {
		return fmt.Errorf("failed to parse cron expression: %w", err)
	}

	s.ticker = time.NewTicker(duration)
	s.stopChan = make(chan struct{})

	go func() {
		for {
			select {
			case t := <-s.ticker.C:
				slog.Info("Cron trigger fired",
					slog.String("name", s.name),
					slog.Time("time", t))

				// Create trigger data with timestamp
				timestampStr := t.Format(time.RFC3339)
				data := map[string]*core.Data{
					"default":   {Value: timestampStr},       // Default output for easy access
					"timestamp": {Value: timestampStr},        // ISO 8601 timestamp
					"schedule":  {Value: s.schedule},          // Original schedule expression
					"unix":      {Value: t.Unix()},            // Unix timestamp
				}
				callback(data)
			case <-s.stopChan:
				slog.Info("Cron trigger stopped", slog.String("name", s.name))
				s.ticker.Stop()
				return
			}
		}
	}()

	slog.Info("Cron trigger started",
		slog.String("name", s.name),
		slog.String("schedule", s.schedule),
		slog.Duration("interval", duration))

	return nil
}

func (s *CronStep) Stop() {
	if s.stopChan != nil {
		close(s.stopChan)
	}
}

// parseCronExpression converts a cron-like expression to a time.Duration
// Supported formats:
// - "@every <duration>" - e.g., "@every 5m", "@every 1h30m"
// - Simple interval formats: "5m", "1h", "30s"
// - Full cron expressions: "* * * * *" (minute hour day month weekday)
func parseCronExpression(expr string) (time.Duration, error) {
	// Handle @every prefix
	if len(expr) > 7 && expr[:7] == "@every " {
		durationStr := expr[7:]
		return time.ParseDuration(durationStr)
	}

	// Try to parse as simple duration
	if d, err := time.ParseDuration(expr); err == nil {
		return d, nil
	}

	// TODO: Implement full cron expression parsing (e.g., "0 */2 * * *")
	// For now, return an error for unsupported formats
	return 0, fmt.Errorf("unsupported cron expression format: %s (use @every <duration> or simple duration like 5m, 1h)", expr)
}

func init() {
	pipeline.RegisterTriggerType("cron", func(name string, config map[string]any) (core.Step, error) {
		schedule, ok := config["schedule"].(string)
		if !ok {
			return nil, fmt.Errorf("cron trigger requires 'schedule' configuration")
		}

		// Validate the schedule expression
		_, err := parseCronExpression(schedule)
		if err != nil {
			return nil, fmt.Errorf("invalid schedule expression: %w", err)
		}

		return &CronStep{
			name:     name,
			schedule: schedule,
		}, nil
	})
}
