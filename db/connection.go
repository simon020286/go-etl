package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

var (
	instance *sql.DB
	once     sync.Once
)

// Config holds database configuration
type Config struct {
	Path     string
	SeedData bool
}

// DefaultConfig returns default database configuration
func DefaultConfig() *Config {
	return &Config{
		Path:     "./data/pipelines.db",
		SeedData: true,
	}
}

// GetDB returns a singleton database instance
func GetDB(config ...*Config) (*sql.DB, error) {
	var err error
	once.Do(func() {
		cfg := DefaultConfig()
		if len(config) > 0 && config[0] != nil {
			cfg = config[0]
		}

		instance, err = initDatabase(cfg)
	})
	return instance, err
}

// initDatabase initializes the database with schema and optional seed data
func initDatabase(config *Config) (*sql.DB, error) {
	// Ensure directory exists
	dir := filepath.Dir(config.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	// Initialize database
	db, err := InitDB(config.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Run migrations
	if err := InitMigrations(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	// Seed data if requested
	if config.SeedData {
		if err := SeedData(db); err != nil {
			// Don't fail if seeding fails, just log it
			fmt.Printf("Warning: failed to seed data: %v\n", err)
		}
	}

	return db, nil
}

// Close closes the database connection
func Close() error {
	if instance != nil {
		return instance.Close()
	}
	return nil
}

// ResetDB closes existing connection and reinitializes (useful for testing)
func ResetDB(config *Config) (*sql.DB, error) {
	if instance != nil {
		instance.Close()
	}
	once = sync.Once{}
	instance = nil
	return GetDB(config)
}

// TestDB returns a database instance for testing (in-memory)
func TestDB() (*sql.DB, error) {
	return InitDB(":memory:")
}