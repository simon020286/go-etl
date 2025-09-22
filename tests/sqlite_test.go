package tests

import (
	"context"
	"fmt"
	"go-etl/pipeline"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var testDBPath string

func TestMain(m *testing.M) {
	// Create temporary database file
	testDBPath = filepath.Join(os.TempDir(), fmt.Sprintf("test_sqlite_%d.db", time.Now().UnixNano()))

	// Run tests
	code := m.Run()

	// Cleanup: remove temporary database file
	os.Remove(testDBPath)

	// Exit with the test result code
	os.Exit(code)
}

func TestSql(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("sqlite")
	if !ok {
		t.Errorf("Step type 'sqlite' not registered")
	}

	stepInstance, err := stepFactory("testSqlite1", map[string]any{
		"connection": testDBPath,
		"query": `CREATE TABLE contacts (
			contact_id INTEGER PRIMARY KEY,
			first_name TEXT NOT NULL,
			last_name TEXT NOT NULL,
			email TEXT NOT NULL UNIQUE,
			phone TEXT NOT NULL UNIQUE
		);`,
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}
	if stepInstance == nil {
		t.Error("Step instance is nil")
	}
	if stepInstance.Name() != "testSqlite1" {
		t.Errorf("Expected step name 'testDelay', got '%s'", stepInstance.Name())
	}

	ctx := context.Background()
	result, err := stepInstance.Run(ctx, nil)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func TestInsertData(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("sqlite")
	if !ok {
		t.Errorf("Step type 'sqlite' not registered")
	}

	stepInstance, err := stepFactory("testSqliteInsert", map[string]any{
		"connection": testDBPath,
		"query":      `INSERT INTO contacts VALUES (1, 'Name', 'Surname', 'email@email.com', '3654112568');`,
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}

	ctx := context.Background()
	result, err := stepInstance.Run(ctx, nil)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}

func TestSelectData(t *testing.T) {
	stepFactory, ok := pipeline.GetStepFactory("sqlite")
	if !ok {
		t.Errorf("Step type 'sqlite' not registered")
	}

	stepInstance, err := stepFactory("testSqliteSelect", map[string]any{
		"connection": testDBPath,
		"query":      `SELECT * FROM contacts;`,
	})

	if err != nil {
		t.Errorf("Failed to create step instance: %v", err)
	}

	ctx := context.Background()
	result, err := stepInstance.Run(ctx, nil)
	if err != nil {
		t.Errorf("Step execution failed: %v", err)
	}
	if result == nil {
		t.Error("Step execution returned nil result")
	} else {
		t.Logf("Step executed successfully, result: %v", result["default"].Value)
	}
}
