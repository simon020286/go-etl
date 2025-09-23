package db

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

const (
	// Pipeline states
	StateCreated   = "CREATED"
	StateRunning   = "RUNNING"
	StatePaused    = "PAUSED"
	StateStopped   = "STOPPED"
	StateError     = "ERROR"
	StateCompleted = "COMPLETED"
)

// InitDB initializes the database with required tables
func InitDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := createTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return db, nil
}

// createTables creates all required tables for pipeline management
func createTables(db *sql.DB) error {
	queries := []string{
		// Migrations table - tracks applied migrations
		`CREATE TABLE IF NOT EXISTS migrations (
			version INTEGER PRIMARY KEY,
			applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,

		// Pipelines table - stores pipeline definitions and metadata
		`CREATE TABLE IF NOT EXISTS pipelines (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			config_yaml TEXT NOT NULL,
			state TEXT DEFAULT 'CREATED',
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			last_run_at DATETIME,
			next_run_at DATETIME,
			enabled BOOLEAN DEFAULT true,
			schedule_cron TEXT,
			tags TEXT -- JSON array of tags
		)`,

		// Executions table - tracks pipeline execution history
		`CREATE TABLE IF NOT EXISTS executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			pipeline_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			completed_at DATETIME,
			duration_ms INTEGER,
			error_message TEXT,
			trigger_type TEXT, -- manual, scheduled, webhook
			trigger_data TEXT, -- JSON data from trigger
			FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
		)`,

		// Execution logs table - stores detailed execution logs
		`CREATE TABLE IF NOT EXISTS execution_logs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			execution_id INTEGER NOT NULL,
			step_name TEXT,
			level TEXT, -- debug, info, warn, error
			message TEXT NOT NULL,
			data TEXT, -- JSON data
			timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (execution_id) REFERENCES executions(id) ON DELETE CASCADE
		)`,

		// Pipeline schedules table - manages recurring executions
		`CREATE TABLE IF NOT EXISTS schedules (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			pipeline_id INTEGER NOT NULL,
			cron_expression TEXT NOT NULL,
			enabled BOOLEAN DEFAULT true,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			last_run_at DATETIME,
			next_run_at DATETIME,
			FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
		)`,

		// Pipeline templates table - stores reusable pipeline templates
		`CREATE TABLE IF NOT EXISTS templates (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			config_yaml TEXT NOT NULL,
			category TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to execute query: %s, error: %w", query, err)
		}
	}

	// Create indexes for better performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_pipelines_state ON pipelines(state)",
		"CREATE INDEX IF NOT EXISTS idx_pipelines_enabled ON pipelines(enabled)",
		"CREATE INDEX IF NOT EXISTS idx_executions_pipeline_id ON executions(pipeline_id)",
		"CREATE INDEX IF NOT EXISTS idx_executions_status ON executions(status)",
		"CREATE INDEX IF NOT EXISTS idx_executions_started_at ON executions(started_at)",
		"CREATE INDEX IF NOT EXISTS idx_execution_logs_execution_id ON execution_logs(execution_id)",
		"CREATE INDEX IF NOT EXISTS idx_execution_logs_timestamp ON execution_logs(timestamp)",
		"CREATE INDEX IF NOT EXISTS idx_schedules_pipeline_id ON schedules(pipeline_id)",
		"CREATE INDEX IF NOT EXISTS idx_schedules_enabled ON schedules(enabled)",
	}

	for _, index := range indexes {
		if _, err := db.Exec(index); err != nil {
			return fmt.Errorf("failed to create index: %s, error: %w", index, err)
		}
	}

	return nil
}

// Pipeline represents a pipeline record in the database
type Pipeline struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	ConfigYAML  string `json:"config_yaml"`
	State       string `json:"state"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	LastRunAt   *string `json:"last_run_at,omitempty"`
	NextRunAt   *string `json:"next_run_at,omitempty"`
	Enabled     bool   `json:"enabled"`
	ScheduleCron *string `json:"schedule_cron,omitempty"`
	Tags        *string `json:"tags,omitempty"`
}

// Execution represents an execution record in the database
type Execution struct {
	ID           int     `json:"id"`
	PipelineID   int     `json:"pipeline_id"`
	Status       string  `json:"status"`
	StartedAt    string  `json:"started_at"`
	CompletedAt  *string `json:"completed_at,omitempty"`
	DurationMs   *int    `json:"duration_ms,omitempty"`
	ErrorMessage *string `json:"error_message,omitempty"`
	TriggerType  string  `json:"trigger_type"`
	TriggerData  *string `json:"trigger_data,omitempty"`
}

// ExecutionLog represents a log entry for an execution
type ExecutionLog struct {
	ID          int     `json:"id"`
	ExecutionID int     `json:"execution_id"`
	StepName    *string `json:"step_name,omitempty"`
	Level       string  `json:"level"`
	Message     string  `json:"message"`
	Data        *string `json:"data,omitempty"`
	Timestamp   string  `json:"timestamp"`
}

// Schedule represents a pipeline schedule
type Schedule struct {
	ID             int     `json:"id"`
	PipelineID     int     `json:"pipeline_id"`
	CronExpression string  `json:"cron_expression"`
	Enabled        bool    `json:"enabled"`
	CreatedAt      string  `json:"created_at"`
	LastRunAt      *string `json:"last_run_at,omitempty"`
	NextRunAt      *string `json:"next_run_at,omitempty"`
}

// Template represents a pipeline template
type Template struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	ConfigYAML  string `json:"config_yaml"`
	Category    string `json:"category"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}