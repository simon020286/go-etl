package db

import (
	"testing"
)

func TestExecutionManager_CRUD(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)
	em := manager.Executions()
	pm := manager.Pipelines()

	// Create a test pipeline first
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "execution-test-pipeline",
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

	// Create execution record manually for testing
	query := `
		INSERT INTO executions (pipeline_id, status, trigger_type)
		VALUES (?, ?, ?)
		RETURNING id
	`
	var executionID int
	err = db.QueryRow(query, pipeline.ID, StateRunning, "manual").Scan(&executionID)
	if err != nil {
		t.Fatalf("Failed to create test execution: %v", err)
	}

	// Test GetExecution
	execution, err := em.GetExecution(executionID)
	if err != nil {
		t.Fatalf("Failed to get execution: %v", err)
	}

	if execution.PipelineID != pipeline.ID {
		t.Errorf("Expected pipeline ID %d, got %d", pipeline.ID, execution.PipelineID)
	}

	if execution.Status != StateRunning {
		t.Errorf("Expected status %s, got %s", StateRunning, execution.Status)
	}

	if execution.TriggerType != "manual" {
		t.Errorf("Expected trigger type 'manual', got %s", execution.TriggerType)
	}

	// Test GetExecution with non-existent ID
	_, err = em.GetExecution(99999)
	if err == nil {
		t.Error("Expected error for non-existent execution")
	}
}

func TestExecutionManager_ListExecutions(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)
	em := manager.Executions()
	pm := manager.Pipelines()

	// Create test pipelines
	pipeline1, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "list-test-pipeline-1",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World 1"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline 1: %v", err)
	}

	pipeline2, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "list-test-pipeline-2",
		ConfigYAML: `
steps:
  - name: test_step
    type: stdout
    config:
      value: "Hello World 2"
`,
	})
	if err != nil {
		t.Fatalf("Failed to create test pipeline 2: %v", err)
	}

	// Create test executions
	executions := []struct {
		pipelineID int
		status     string
		triggerType string
	}{
		{pipeline1.ID, StateRunning, "manual"},
		{pipeline1.ID, StateCompleted, "scheduled"},
		{pipeline2.ID, StateError, "webhook"},
		{pipeline2.ID, StateCompleted, "manual"},
	}

	for _, exec := range executions {
		query := `
			INSERT INTO executions (pipeline_id, status, trigger_type)
			VALUES (?, ?, ?)
		`
		_, err = db.Exec(query, exec.pipelineID, exec.status, exec.triggerType)
		if err != nil {
			t.Fatalf("Failed to create test execution: %v", err)
		}
	}

	// Test listing all executions
	allExecs, total, err := em.ListExecutions(ListExecutionsRequest{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("Failed to list executions: %v", err)
	}

	if len(allExecs) != 4 {
		t.Errorf("Expected 4 executions, got %d", len(allExecs))
	}

	if total != 4 {
		t.Errorf("Expected total 4, got %d", total)
	}

	// Test filtering by pipeline
	pipeline1Execs, pipeline1Total, err := em.ListExecutions(ListExecutionsRequest{
		PipelineID: pipeline1.ID,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("Failed to list pipeline 1 executions: %v", err)
	}

	if len(pipeline1Execs) != 2 {
		t.Errorf("Expected 2 executions for pipeline 1, got %d", len(pipeline1Execs))
	}

	if pipeline1Total != 2 {
		t.Errorf("Expected pipeline 1 total 2, got %d", pipeline1Total)
	}

	// Test filtering by status
	completedExecs, completedTotal, err := em.ListExecutions(ListExecutionsRequest{
		Status: StateCompleted,
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("Failed to list completed executions: %v", err)
	}

	if len(completedExecs) != 2 {
		t.Errorf("Expected 2 completed executions, got %d", len(completedExecs))
	}

	if completedTotal != 2 {
		t.Errorf("Expected completed total 2, got %d", completedTotal)
	}

	// Test pagination
	page1, _, err := em.ListExecutions(ListExecutionsRequest{
		Limit:  2,
		Offset: 0,
	})
	if err != nil {
		t.Fatalf("Failed to list executions page 1: %v", err)
	}

	if len(page1) != 2 {
		t.Errorf("Expected 2 executions on page 1, got %d", len(page1))
	}

	page2, _, err := em.ListExecutions(ListExecutionsRequest{
		Limit:  2,
		Offset: 2,
	})
	if err != nil {
		t.Fatalf("Failed to list executions page 2: %v", err)
	}

	if len(page2) != 2 {
		t.Errorf("Expected 2 executions on page 2, got %d", len(page2))
	}
}

func TestExecutionManager_GetExecutionLogs(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)
	em := manager.Executions()
	pm := manager.Pipelines()

	// Create test pipeline and execution
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "logs-test-pipeline",
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

	// Create execution
	var executionID int
	query := `
		INSERT INTO executions (pipeline_id, status, trigger_type)
		VALUES (?, ?, ?)
		RETURNING id
	`
	err = db.QueryRow(query, pipeline.ID, StateRunning, "manual").Scan(&executionID)
	if err != nil {
		t.Fatalf("Failed to create test execution: %v", err)
	}

	// Create test logs
	logs := []struct {
		stepName string
		level    string
		message  string
	}{
		{"step1", "info", "Step started"},
		{"step1", "debug", "Processing data"},
		{"step1", "info", "Step completed"},
		{"step2", "error", "Step failed"},
	}

	for _, log := range logs {
		query = `
			INSERT INTO execution_logs (execution_id, step_name, level, message)
			VALUES (?, ?, ?, ?)
		`
		_, err = db.Exec(query, executionID, log.stepName, log.level, log.message)
		if err != nil {
			t.Fatalf("Failed to create test log: %v", err)
		}
	}

	// Test getting execution logs
	retrievedLogs, err := em.GetExecutionLogs(executionID, 100)
	if err != nil {
		t.Fatalf("Failed to get execution logs: %v", err)
	}

	if len(retrievedLogs) != 4 {
		t.Errorf("Expected 4 logs, got %d", len(retrievedLogs))
	}

	// Verify log order (should be chronological)
	if retrievedLogs[0].Message != "Step started" {
		t.Errorf("Expected first log message 'Step started', got '%s'", retrievedLogs[0].Message)
	}

	// Test with limit
	limitedLogs, err := em.GetExecutionLogs(executionID, 2)
	if err != nil {
		t.Fatalf("Failed to get limited execution logs: %v", err)
	}

	if len(limitedLogs) != 2 {
		t.Errorf("Expected 2 logs with limit, got %d", len(limitedLogs))
	}
}

func TestExecutionManager_DeleteExecution(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)
	em := manager.Executions()
	pm := manager.Pipelines()

	// Create test pipeline and execution
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "delete-test-pipeline",
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

	var executionID int
	query := `
		INSERT INTO executions (pipeline_id, status, trigger_type)
		VALUES (?, ?, ?)
		RETURNING id
	`
	err = db.QueryRow(query, pipeline.ID, StateCompleted, "manual").Scan(&executionID)
	if err != nil {
		t.Fatalf("Failed to create test execution: %v", err)
	}

	// Delete the execution
	err = em.DeleteExecution(executionID)
	if err != nil {
		t.Fatalf("Failed to delete execution: %v", err)
	}

	// Verify it's gone
	_, err = em.GetExecution(executionID)
	if err == nil {
		t.Error("Expected error when getting deleted execution")
	}

	// Test deleting non-existent execution
	err = em.DeleteExecution(99999)
	if err == nil {
		t.Error("Expected error when deleting non-existent execution")
	}
}

func TestExecutionManager_GetExecutionStats(t *testing.T) {
	db, err := TestDB()
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	manager := GetManagerWithDB(db)
	em := manager.Executions()
	pm := manager.Pipelines()

	// Create test pipeline
	pipeline, err := pm.CreatePipeline(CreatePipelineRequest{
		Name: "stats-test-pipeline",
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

	// Create test executions with different statuses and durations
	executions := []struct {
		status     string
		durationMs *int
	}{
		{StateCompleted, intPtr(1000)},
		{StateCompleted, intPtr(2000)},
		{StateError, intPtr(500)},
		{StateRunning, nil},
		{StateCompleted, intPtr(1500)},
	}

	for _, exec := range executions {
		query := `
			INSERT INTO executions (pipeline_id, status, trigger_type, duration_ms, completed_at)
			VALUES (?, ?, ?, ?, CASE WHEN ? IS NOT NULL THEN CURRENT_TIMESTAMP ELSE NULL END)
		`
		_, err = db.Exec(query, pipeline.ID, exec.status, "manual", exec.durationMs, exec.durationMs)
		if err != nil {
			t.Fatalf("Failed to create test execution: %v", err)
		}
	}

	// Get execution stats
	stats, err := em.GetExecutionStats(pipeline.ID)
	if err != nil {
		t.Fatalf("Failed to get execution stats: %v", err)
	}

	if stats.TotalExecutions != 5 {
		t.Errorf("Expected 5 total executions, got %d", stats.TotalExecutions)
	}

	if stats.CompletedExecutions != 3 {
		t.Errorf("Expected 3 completed executions, got %d", stats.CompletedExecutions)
	}

	if stats.ErrorExecutions != 1 {
		t.Errorf("Expected 1 error execution, got %d", stats.ErrorExecutions)
	}

	if stats.RunningExecutions != 1 {
		t.Errorf("Expected 1 running execution, got %d", stats.RunningExecutions)
	}

	// Check duration stats (average of 1000, 2000, 500, 1500 = 1250)
	if stats.AvgDurationMs == nil || *stats.AvgDurationMs != 1250 {
		t.Errorf("Expected avg duration 1250ms, got %v", stats.AvgDurationMs)
	}

	if stats.MaxDurationMs == nil || *stats.MaxDurationMs != 2000 {
		t.Errorf("Expected max duration 2000ms, got %v", stats.MaxDurationMs)
	}

	if stats.MinDurationMs == nil || *stats.MinDurationMs != 500 {
		t.Errorf("Expected min duration 500ms, got %v", stats.MinDurationMs)
	}
}

// Helper function to create int pointer
func intPtr(i int) *int {
	return &i
}