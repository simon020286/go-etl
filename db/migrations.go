package db

import (
	"database/sql"
	"fmt"
	"time"
)

// Migration represents a database migration
type Migration struct {
	Version     int
	Description string
	Up          string
	Down        string
}

// GetMigrations returns all available migrations
func GetMigrations() []Migration {
	return []Migration{
		{
			Version:     1,
			Description: "Initial pipeline management schema",
			Up: `
				-- This migration is handled by schema.go createTables function
				-- Kept for future migration tracking
				SELECT 1;
			`,
			Down: `
				DROP TABLE IF EXISTS execution_logs;
				DROP TABLE IF EXISTS executions;
				DROP TABLE IF EXISTS schedules;
				DROP TABLE IF EXISTS templates;
				DROP TABLE IF EXISTS pipelines;
				DROP TABLE IF EXISTS migrations;
			`,
		},
	}
}

// MigrationRecord tracks applied migrations
type MigrationRecord struct {
	Version   int       `json:"version"`
	AppliedAt time.Time `json:"applied_at"`
}

// InitMigrations creates the migrations table and applies pending migrations
func InitMigrations(db *sql.DB) error {
	// Create migrations table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS migrations (
			version INTEGER PRIMARY KEY,
			applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get applied migrations
	applied, err := getAppliedMigrations(db)
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// Apply pending migrations
	migrations := GetMigrations()
	for _, migration := range migrations {
		if !contains(applied, migration.Version) {
			if err := applyMigration(db, migration); err != nil {
				return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
			}
		}
	}

	return nil
}

// getAppliedMigrations returns list of applied migration versions
func getAppliedMigrations(db *sql.DB) ([]int, error) {
	rows, err := db.Query("SELECT version FROM migrations ORDER BY version")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}

	return versions, nil
}

// applyMigration applies a single migration
func applyMigration(db *sql.DB, migration Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Execute migration
	if _, err := tx.Exec(migration.Up); err != nil {
		return err
	}

	// Record migration
	_, err = tx.Exec("INSERT INTO migrations (version) VALUES (?)", migration.Version)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// contains checks if slice contains value
func contains(slice []int, value int) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

// SeedData inserts initial data for development/testing
func SeedData(db *sql.DB) error {
	// Insert sample templates
	templates := []struct {
		name, description, category, config string
	}{
		{
			"Simple Stdout",
			"Basic pipeline that outputs a message",
			"basic",
			`steps:
  - name: hello
    type: stdout
    config:
      value: "Hello, World!"`,
		},
		{
			"File Processing",
			"Read file, transform, and output",
			"data",
			`steps:
  - name: read_file
    type: file
    config:
      path: "/tmp/input.txt"
  - name: transform
    type: map
    inputs:
      - read_file
    config:
      fields:
        - name: processed
          value: "ctx.read_file.default"
  - name: output
    type: stdout
    inputs:
      - transform
    config:
      value: "ctx.transform.processed"`,
		},
		{
			"HTTP API Call",
			"Make HTTP request and process response",
			"api",
			`steps:
  - name: api_call
    type: http_client
    config:
      url: "https://api.example.com/data"
      method: "GET"
  - name: process
    type: stdout
    inputs:
      - api_call
    config:
      value: "API Response: ctx.api_call.default"`,
		},
	}

	for _, template := range templates {
		_, err := db.Exec(`
			INSERT OR IGNORE INTO templates (name, description, category, config_yaml)
			VALUES (?, ?, ?, ?)
		`, template.name, template.description, template.category, template.config)
		if err != nil {
			return fmt.Errorf("failed to insert template %s: %w", template.name, err)
		}
	}

	return nil
}