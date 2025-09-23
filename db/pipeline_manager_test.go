package db

import (
	"testing"
	"time"
)

func TestPipelineManager_CreatePipeline(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	validConfig := `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`

	tests := []struct {
		name      string
		request   CreatePipelineRequest
		expectErr bool
	}{
		{
			name: "valid pipeline",
			request: CreatePipelineRequest{
				Name:        "test-pipeline",
				Description: "Test pipeline",
				ConfigYAML:  validConfig,
				Tags:        []string{"test", "demo"},
			},
			expectErr: false,
		},
		{
			name: "invalid YAML",
			request: CreatePipelineRequest{
				Name:       "invalid-pipeline",
				ConfigYAML: "invalid: yaml: content:",
			},
			expectErr: true,
		},
		{
			name: "missing steps",
			request: CreatePipelineRequest{
				Name:       "no-steps-pipeline",
				ConfigYAML: "triggers: []",
			},
			expectErr: true,
		},
		{
			name: "empty steps",
			request: CreatePipelineRequest{
				Name:       "empty-steps-pipeline",
				ConfigYAML: "steps: []",
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := pm.CreatePipeline(tt.request)
			if tt.expectErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if pipeline.Name != tt.request.Name {
				t.Errorf("Expected name %s, got %s", tt.request.Name, pipeline.Name)
			}

			if pipeline.State != StateCreated {
				t.Errorf("Expected state %s, got %s", StateCreated, pipeline.State)
			}

			if !pipeline.Enabled {
				t.Error("Expected pipeline to be enabled by default")
			}
		})
	}
}

func TestPipelineManager_GetPipeline(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create a test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name:        "get-test-pipeline",
		Description: "Pipeline for get test",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline: %v", err)
	}

	// Test GetPipeline by ID
	retrieved, err := pm.GetPipeline(pipeline.ID)
	if err != nil {
		t.Fatalf("Failed to get pipeline by ID: %v", err)
	}

	if retrieved.Name != pipeline.Name {
		t.Errorf("Expected name %s, got %s", pipeline.Name, retrieved.Name)
	}

	// Test GetPipelineByName
	retrievedByName, err := pm.GetPipelineByName(pipeline.Name)
	if err != nil {
		t.Fatalf("Failed to get pipeline by name: %v", err)
	}

	if retrievedByName.ID != pipeline.ID {
		t.Errorf("Expected ID %d, got %d", pipeline.ID, retrievedByName.ID)
	}

	// Test non-existent pipeline
	_, err = pm.GetPipeline(99999)
	if err == nil {
		t.Error("Expected error for non-existent pipeline")
	}

	_, err = pm.GetPipelineByName("non-existent")
	if err == nil {
		t.Error("Expected error for non-existent pipeline")
	}
}

func TestPipelineManager_UpdatePipeline(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create a test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name:        "update-test-pipeline",
		Description: "Original description",
		ConfigYAML: `
steps:
  - name: original_step
    type: stdout
    config:
      value: "Original"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline: %v", err)
	}

	// Test update
	newName := "updated-pipeline"
	newDescription := "Updated description"
	newConfig := `
steps:
  - name: updated_step
    type: stdout
    config:
      value: "Updated"
`
	enabled := false

	updated, err := pm.UpdatePipeline(pipeline.ID, UpdatePipelineRequest{
		Name:        &newName,
		Description: &newDescription,
		ConfigYAML:  &newConfig,
		Enabled:     &enabled,
		Tags:        []string{"updated", "test"},
	})
	if err != nil {
		t.Fatalf("Failed to update pipeline: %v", err)
	}

	if updated.Name != newName {
		t.Errorf("Expected name %s, got %s", newName, updated.Name)
	}

	if updated.Description != newDescription {
		t.Errorf("Expected description %s, got %s", newDescription, updated.Description)
	}

	if updated.Enabled {
		t.Error("Expected pipeline to be disabled")
	}

	// Test partial update
	partialDescription := "Partially updated"
	partialUpdated, err := pm.UpdatePipeline(pipeline.ID, UpdatePipelineRequest{
		Description: &partialDescription,
	})
	if err != nil {
		t.Fatalf("Failed to partially update pipeline: %v", err)
	}

	if partialUpdated.Description != partialDescription {
		t.Errorf("Expected description %s, got %s", partialDescription, partialUpdated.Description)
	}

	// Name should remain the same
	if partialUpdated.Name != newName {
		t.Errorf("Expected name to remain %s, got %s", newName, partialUpdated.Name)
	}
}

func TestPipelineManager_DeletePipeline(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create a test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name:        "delete-test-pipeline",
		Description: "Pipeline to delete",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline: %v", err)
	}

	// Delete the pipeline
	err = pm.DeletePipeline(pipeline.ID)
	if err != nil {
		t.Fatalf("Failed to delete pipeline: %v", err)
	}

	// Verify it's gone
	_, err = pm.GetPipeline(pipeline.ID)
	if err == nil {
		t.Error("Expected error when getting deleted pipeline")
	}

	// Test deleting non-existent pipeline
	err = pm.DeletePipeline(99999)
	if err == nil {
		t.Error("Expected error when deleting non-existent pipeline")
	}
}

func TestPipelineManager_ListPipelines(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create test pipelines
	enabled := true
	disabled := false

	pipelines := []CreatePipelineRequest{
		{
			Name:    "pipeline-1",
			Enabled: &enabled,
			ConfigYAML: `
steps:
  - name: step1
    type: stdout
    config:
      value: "Pipeline 1"
`,
		},
		{
			Name:    "pipeline-2",
			Enabled: &disabled,
			ConfigYAML: `
steps:
  - name: step2
    type: stdout
    config:
      value: "Pipeline 2"
`,
		},
		{
			Name:    "pipeline-3",
			Enabled: &enabled,
			ConfigYAML: `
steps:
  - name: step3
    type: stdout
    config:
      value: "Pipeline 3"
`,
		},
	}

	var createdIDs []int
	for _, req := range pipelines {
		pipeline, err := pm.CreatePipeline(req)
		if err != nil {
			t.Fatalf("Failed to create test pipeline: %v", err)
		}
		createdIDs = append(createdIDs, pipeline.ID)
	}

	// Test listing all pipelines
	all, total, err := pm.ListPipelines(ListPipelinesRequest{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Failed to list pipelines: %v", err)
	}

	if len(all) != 3 {
		t.Errorf("Expected 3 pipelines, got %d", len(all))
	}

	if total != 3 {
		t.Errorf("Expected total 3, got %d", total)
	}

	// Test filtering by enabled status
	enabledOnly, enabledTotal, err := pm.ListPipelines(ListPipelinesRequest{
		Enabled: &enabled,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("Failed to list enabled pipelines: %v", err)
	}

	if len(enabledOnly) != 2 {
		t.Errorf("Expected 2 enabled pipelines, got %d", len(enabledOnly))
	}

	if enabledTotal != 2 {
		t.Errorf("Expected enabled total 2, got %d", enabledTotal)
	}

	// Test pagination
	page1, _, err := pm.ListPipelines(ListPipelinesRequest{
		Limit:  1,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Failed to list pipelines page 1: %v", err)
	}

	if len(page1) != 1 {
		t.Errorf("Expected 1 pipeline on page 1, got %d", len(page1))
	}

	page2, _, err := pm.ListPipelines(ListPipelinesRequest{
		Limit:  1,
		Offset: 1,
	})
	if err != nil {
		t.Fatalf("Failed to list pipelines page 2: %v", err)
	}

	if len(page2) != 1 {
		t.Errorf("Expected 1 pipeline on page 2, got %d", len(page2))
	}

	// Ensure different pipelines on different pages
	if page1[0].ID == page2[0].ID {
		t.Error("Expected different pipelines on different pages")
	}
}

func TestPipelineManager_UpdatePipelineState(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create a test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "state-test-pipeline",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline: %v", err)
	}

	// Test valid state updates
	validStates := []string{StateRunning, StatePaused, StateStopped, StateError, StateCompleted}
	for _, state := range validStates {
		err = pm.UpdatePipelineState(pipeline.ID, state)
		if err != nil {
			t.Fatalf("Failed to update pipeline state to %s: %v", state, err)
		}

		updated, err := pm.GetPipeline(pipeline.ID)
		if err != nil {
			t.Fatalf("Failed to get updated pipeline: %v", err)
		}

		if updated.State != state {
			t.Errorf("Expected state %s, got %s", state, updated.State)
		}
	}

	// Test invalid state
	err = pm.UpdatePipelineState(pipeline.ID, "INVALID_STATE")
	if err == nil {
		t.Error("Expected error for invalid state")
	}

	// Test non-existent pipeline
	err = pm.UpdatePipelineState(99999, StateRunning)
	if err == nil {
		t.Error("Expected error for non-existent pipeline")
	}
}

func TestPipelineManager_UpdatePipelineLastRun(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	pm := NewPipelineManager(db)

	// Create a test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "lastrun-test-pipeline",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline: %v", err)
	}

	// Update last run time
	runTime := time.Now().UTC()
	err = pm.UpdatePipelineLastRun(pipeline.ID, runTime)
	if err != nil {
		t.Fatalf("Failed to update pipeline last run: %v", err)
	}

	// Verify update
	updated, err := pm.GetPipeline(pipeline.ID)
	if err != nil {
		t.Fatalf("Failed to get updated pipeline: %v", err)
	}

	if updated.LastRunAt == nil {
		t.Error("Expected LastRunAt to be set")
	}

	// Test non-existent pipeline
	err = pm.UpdatePipelineLastRun(99999, runTime)
	if err == nil {
		t.Error("Expected error for non-existent pipeline")
	}
}