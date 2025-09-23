package db

import (
	"testing"
)

func TestManager_Singleton(t *testing.T) {
	// Reset manager for clean test
	ResetManager()

	// Get manager instances
	manager1, err := GetManager()
	if err != nil {
		t.Fatalf("Failed to get manager: %v", err)
	}

	manager2, err := GetManager()
	if err != nil {
		t.Fatalf("Failed to get manager: %v", err)
	}

	// Should be the same instance
	if manager1 != manager2 {
		t.Error("Expected same manager instance")
	}

	// Cleanup
	manager1.Close()
}

func TestManager_Pipelines(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)

	// Get pipeline manager
	pm1 := manager.Pipelines()
	pm2 := manager.Pipelines()

	// Should be the same instance
	if pm1 != pm2 {
		t.Error("Expected same pipeline manager instance")
	}

	// Test basic functionality
	pipeline, err := pm1.CreatePipeline(CreatePipelineRequest{
		Name: "test-pipeline",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	// Retrieve with second manager instance
	retrieved, err := pm2.GetPipeline(pipeline.ID)
	if err != nil {
		t.Fatalf("Failed to get pipeline: %v", err)
	}

	if retrieved.Name != pipeline.Name {
		t.Errorf("Expected name %s, got %s", pipeline.Name, retrieved.Name)
	}
}

func TestManager_DB(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)

	// Should return the same DB instance
	if manager.DB() != db {
		t.Error("Expected same DB instance")
	}
}