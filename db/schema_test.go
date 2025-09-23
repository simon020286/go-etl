package db

import (
	"testing"
)

func TestInitDB(t *testing.T) {
	// Test with in-memory database
	db, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Verify tables exist by querying them
	tables := []string{"pipelines", "executions", "execution_logs", "schedules", "templates", "migrations"}

	for _, table := range tables {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
		if err != nil {
			t.Errorf("Failed to query table %s: %v", table, err)
		}
	}
}

func TestMigrations(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Test migrations
	err = InitMigrations(db)
	if err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Verify migration record exists
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query migrations: %v", err)
	}

	if count == 0 {
		t.Error("Expected at least one migration record")
	}
}

func TestSeedData(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	// Seed data
	err = SeedData(db)
	if err != nil {
		t.Fatalf("Failed to seed data: %v", err)
	}

	// Verify templates were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM templates").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query templates: %v", err)
	}

	if count == 0 {
		t.Error("Expected at least one template record")
	}
}

func TestDatabaseConfig(t *testing.T) {
	// Test default config
	config := DefaultConfig()
	if config.Path != "./data/pipelines.db" {
		t.Errorf("Expected default path './data/pipelines.db', got %s", config.Path)
	}
	if !config.SeedData {
		t.Error("Expected SeedData to be true by default")
	}
}