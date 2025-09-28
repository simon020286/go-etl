package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go-etl/core"
	"go-etl/pipeline"
)

// PipelineStateManager handles pipeline state transitions and execution tracking
type PipelineStateManager struct {
	db              *sql.DB
	pipelineManager *PipelineManager
	runningPipelines map[int]*RunningPipeline
	mu              sync.RWMutex
	eventListeners  []StateEventListener
}

// RunningPipeline tracks an active pipeline execution
type RunningPipeline struct {
	ID          int
	Pipeline    *pipeline.Pipeline
	Execution   *Execution
	Context     context.Context
	CancelFunc  context.CancelFunc
	StartTime   time.Time
	Status      string
	mu          sync.RWMutex
}

// StateEvent represents a pipeline state change event
type StateEvent struct {
	PipelineID   int                `json:"pipeline_id"`
	PipelineName string             `json:"pipeline_name"`
	OldState     string             `json:"old_state"`
	NewState     string             `json:"new_state"`
	Timestamp    time.Time          `json:"timestamp"`
	ExecutionID  *int               `json:"execution_id,omitempty"`
	Data         map[string]*core.Data `json:"data,omitempty"`
	Error        string             `json:"error,omitempty"`
}

// StateEventListener defines the interface for state event listeners
type StateEventListener interface {
	OnStateChange(event StateEvent)
}

// StateEventListenerFunc is a function adapter for StateEventListener
type StateEventListenerFunc func(event StateEvent)

func (f StateEventListenerFunc) OnStateChange(event StateEvent) {
	f(event)
}

// NewPipelineStateManager creates a new pipeline state manager
func NewPipelineStateManager(db *sql.DB, pipelineManager *PipelineManager) *PipelineStateManager {
	return &PipelineStateManager{
		db:              db,
		pipelineManager: pipelineManager,
		runningPipelines: make(map[int]*RunningPipeline),
		eventListeners:  make([]StateEventListener, 0),
	}
}

// AddStateListener adds a listener for state change events
func (psm *PipelineStateManager) AddStateListener(listener StateEventListener) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	psm.eventListeners = append(psm.eventListeners, listener)
}

// emitStateEvent emits a state change event to all listeners
func (psm *PipelineStateManager) emitStateEvent(event StateEvent) {
	psm.mu.RLock()
	listeners := make([]StateEventListener, len(psm.eventListeners))
	copy(listeners, psm.eventListeners)
	psm.mu.RUnlock()

	for _, listener := range listeners {
		go listener.OnStateChange(event)
	}
}

// StartPipeline starts a pipeline execution
func (psm *PipelineStateManager) StartPipeline(pipelineID int, triggerType, triggerData string) (*Execution, error) {
	fmt.Printf("[DEBUG] StartPipeline: Starting pipeline %d with trigger %s\n", pipelineID, triggerType)

	// Check if pipeline is already running
	fmt.Printf("[DEBUG] StartPipeline: Checking if pipeline is already running\n")
	psm.mu.RLock()
	if _, isRunning := psm.runningPipelines[pipelineID]; isRunning {
		psm.mu.RUnlock()
		fmt.Printf("[DEBUG] StartPipeline: Pipeline is already running, returning error\n")
		return nil, fmt.Errorf("pipeline %d is already running", pipelineID)
	}
	psm.mu.RUnlock()
	fmt.Printf("[DEBUG] StartPipeline: Pipeline not running, continuing\n")

	// Get pipeline from database
	fmt.Printf("[DEBUG] StartPipeline: Getting pipeline from database\n")
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		fmt.Printf("[DEBUG] StartPipeline: Failed to get pipeline: %v\n", err)
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}
	fmt.Printf("[DEBUG] StartPipeline: Got pipeline record: %s\n", pipelineRecord.Name)

	fmt.Printf("[DEBUG] StartPipeline: Checking if pipeline is enabled\n")
	if !pipelineRecord.Enabled {
		fmt.Printf("[DEBUG] StartPipeline: Pipeline is disabled, returning error\n")
		return nil, fmt.Errorf("pipeline %d is disabled", pipelineID)
	}
	fmt.Printf("[DEBUG] StartPipeline: Pipeline is enabled, continuing\n")

	// Skip pipeline loading for API testing (TODO: Fix pipeline loading)
	// For now, we'll create a mock pipeline execution without actually loading the YAML
	fmt.Printf("[DEBUG] StartPipeline: Creating mock pipeline config\n")
	config := (*pipeline.Pipeline)(nil) // Mock pipeline

	// Create execution record
	fmt.Printf("[DEBUG] StartPipeline: Creating execution record\n")
	execution, err := psm.createExecution(pipelineID, triggerType, triggerData)
	if err != nil {
		fmt.Printf("[DEBUG] StartPipeline: Failed to create execution: %v\n", err)
		return nil, fmt.Errorf("failed to create execution record: %w", err)
	}
	fmt.Printf("[DEBUG] StartPipeline: Created execution with ID %d\n", execution.ID)

	// Update pipeline state to RUNNING
	fmt.Printf("[DEBUG] StartPipeline: Updating pipeline state to RUNNING\n")
	oldState := pipelineRecord.State
	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StateRunning)
	if err != nil {
		fmt.Printf("[DEBUG] StartPipeline: Failed to update pipeline state: %v\n", err)
		// Clean up execution record
		psm.updateExecution(execution.ID, StateError, nil, err.Error())
		return nil, fmt.Errorf("failed to update pipeline state: %w", err)
	}
	fmt.Printf("[DEBUG] StartPipeline: Pipeline state updated successfully\n")

	// Create running pipeline context
	fmt.Printf("[DEBUG] StartPipeline: Creating running pipeline context\n")
	ctx, cancel := context.WithCancel(context.Background())
	runningPipeline := &RunningPipeline{
		ID:         pipelineID,
		Pipeline:   config,
		Execution:  execution,
		Context:    ctx,
		CancelFunc: cancel,
		StartTime:  time.Now(),
		Status:     StateRunning,
	}
	fmt.Printf("[DEBUG] StartPipeline: Created running pipeline struct\n")

	// Skip event handler setup for mock pipeline
	// config.OnChange = func(event core.ChangeEvent) {
	//	psm.logExecutionEvent(execution.ID, event)
	// }

	// Register running pipeline
	fmt.Printf("[DEBUG] StartPipeline: Registering running pipeline\n")
	psm.mu.Lock()
	psm.runningPipelines[pipelineID] = runningPipeline
	psm.mu.Unlock()
	fmt.Printf("[DEBUG] StartPipeline: Running pipeline registered\n")

	// Emit state change event
	fmt.Printf("[DEBUG] StartPipeline: Emitting state change event\n")
	psm.emitStateEvent(StateEvent{
		PipelineID:   pipelineID,
		PipelineName: pipelineRecord.Name,
		OldState:     oldState,
		NewState:     StateRunning,
		Timestamp:    time.Now(),
		ExecutionID:  &execution.ID,
	})
	fmt.Printf("[DEBUG] StartPipeline: State change event emitted\n")

	// Start pipeline execution in goroutine
	fmt.Printf("[DEBUG] StartPipeline: Starting pipeline execution in goroutine\n")
	go psm.executePipeline(runningPipeline, pipelineRecord)

	fmt.Printf("[DEBUG] StartPipeline: Returning execution, pipeline started successfully\n")
	return execution, nil
}

// StopPipeline stops a running pipeline
func (psm *PipelineStateManager) StopPipeline(pipelineID int) error {
	psm.mu.RLock()
	runningPipeline, exists := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %d is not running", pipelineID)
	}

	// Cancel the pipeline context
	runningPipeline.CancelFunc()

	// Update status
	runningPipeline.mu.Lock()
	runningPipeline.Status = StateStopped
	runningPipeline.mu.Unlock()

	return nil
}

// PausePipeline pauses a running pipeline (implementation depends on pipeline engine capabilities)
func (psm *PipelineStateManager) PausePipeline(pipelineID int) error {
	psm.mu.RLock()
	runningPipeline, exists := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %d is not running", pipelineID)
	}

	// Update status to paused
	runningPipeline.mu.Lock()
	runningPipeline.Status = StatePaused
	runningPipeline.mu.Unlock()

	// Update database state
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return err
	}

	oldState := pipelineRecord.State
	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StatePaused)
	if err != nil {
		return err
	}

	// Emit state change event
	psm.emitStateEvent(StateEvent{
		PipelineID:   pipelineID,
		PipelineName: pipelineRecord.Name,
		OldState:     oldState,
		NewState:     StatePaused,
		Timestamp:    time.Now(),
		ExecutionID:  &runningPipeline.Execution.ID,
	})

	return nil
}

// ResumePipeline resumes a paused pipeline
func (psm *PipelineStateManager) ResumePipeline(pipelineID int) error {
	psm.mu.RLock()
	runningPipeline, exists := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %d is not running", pipelineID)
	}

	runningPipeline.mu.RLock()
	currentStatus := runningPipeline.Status
	runningPipeline.mu.RUnlock()

	if currentStatus != StatePaused {
		return fmt.Errorf("pipeline %d is not paused", pipelineID)
	}

	// Update status back to running
	runningPipeline.mu.Lock()
	runningPipeline.Status = StateRunning
	runningPipeline.mu.Unlock()

	// Update database state
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return err
	}

	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StateRunning)
	if err != nil {
		return err
	}

	// Emit state change event
	psm.emitStateEvent(StateEvent{
		PipelineID:   pipelineID,
		PipelineName: pipelineRecord.Name,
		OldState:     StatePaused,
		NewState:     StateRunning,
		Timestamp:    time.Now(),
		ExecutionID:  &runningPipeline.Execution.ID,
	})

	return nil
}

// GetRunningPipelines returns all currently running pipelines
func (psm *PipelineStateManager) GetRunningPipelines() map[int]*RunningPipeline {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	result := make(map[int]*RunningPipeline)
	for id, rp := range psm.runningPipelines {
		result[id] = rp
	}
	return result
}

// IsRunning checks if a pipeline is currently running
func (psm *PipelineStateManager) IsRunning(pipelineID int) bool {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	_, exists := psm.runningPipelines[pipelineID]
	return exists
}

// GetPipelineStatus returns the current status of a pipeline
func (psm *PipelineStateManager) GetPipelineStatus(pipelineID int) (string, error) {
	// Check if running
	psm.mu.RLock()
	runningPipeline, isRunning := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if isRunning {
		runningPipeline.mu.RLock()
		status := runningPipeline.Status
		runningPipeline.mu.RUnlock()
		return status, nil
	}

	// Get from database
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return "", err
	}

	return pipelineRecord.State, nil
}

// executePipeline runs the actual pipeline execution
func (psm *PipelineStateManager) executePipeline(runningPipeline *RunningPipeline, pipelineRecord *Pipeline) {
	defer func() {
		// Clean up running pipeline
		psm.mu.Lock()
		delete(psm.runningPipelines, runningPipeline.ID)
		psm.mu.Unlock()
	}()

	var finalState string
	var errorMsg string

	// SIMPLIFIED MOCK implementation for API testing - instant completion
	// TODO: Replace with actual pipeline execution when step engine is ready

	// Check if context was cancelled immediately
	select {
	case <-runningPipeline.Context.Done():
		finalState = StateStopped
		errorMsg = "Pipeline execution was stopped"
	default:
		// Simulate instant successful completion for testing
		finalState = StateCompleted
	}

	duration := time.Since(runningPipeline.StartTime)
	durationMs := int(duration.Milliseconds())

	// Update execution record
	psm.updateExecution(runningPipeline.Execution.ID, finalState, &durationMs, errorMsg)

	// Update pipeline state in database
	err := psm.pipelineManager.UpdatePipelineState(runningPipeline.ID, finalState)
	if err != nil {
		fmt.Printf("[ERROR] executePipeline: Failed to update pipeline state to %s: %v\n", finalState, err)
	}

	// Emit final state change event
	psm.emitStateEvent(StateEvent{
		PipelineID:   runningPipeline.ID,
		PipelineName: pipelineRecord.Name,
		OldState:     StateRunning,
		NewState:     finalState,
		Timestamp:    time.Now(),
		ExecutionID:  &runningPipeline.Execution.ID,
		Error:        errorMsg,
	})
}

// createExecution creates a new execution record
func (psm *PipelineStateManager) createExecution(pipelineID int, triggerType, triggerData string) (*Execution, error) {
	// Use simpler INSERT approach for SQLite compatibility
	var triggerDataPtr *string
	if triggerData != "" {
		triggerDataPtr = &triggerData
	}

	// Insert execution record
	insertQuery := `
		INSERT INTO executions (pipeline_id, status, trigger_type, trigger_data, started_at)
		VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
	`

	result, err := psm.db.Exec(insertQuery, pipelineID, StateRunning, triggerType, triggerDataPtr)
	if err != nil {
		return nil, fmt.Errorf("failed to insert execution: %w", err)
	}

	executionID, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get execution ID: %w", err)
	}

	// Get the created execution
	selectQuery := `
		SELECT id, pipeline_id, status, started_at, completed_at, duration_ms, error_message, trigger_type, trigger_data
		FROM executions WHERE id = ?
	`

	var execution Execution
	err = psm.db.QueryRow(selectQuery, executionID).Scan(
		&execution.ID, &execution.PipelineID, &execution.Status,
		&execution.StartedAt, &execution.CompletedAt, &execution.DurationMs,
		&execution.ErrorMessage, &execution.TriggerType, &execution.TriggerData,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to retrieve execution: %w", err)
	}

	return &execution, nil
}

// updateExecution updates an execution record
func (psm *PipelineStateManager) updateExecution(executionID int, status string, durationMs *int, errorMsg string) error {
	var errorMsgPtr *string
	if errorMsg != "" {
		errorMsgPtr = &errorMsg
	}

	query := `
		UPDATE executions
		SET status = ?, completed_at = CURRENT_TIMESTAMP, duration_ms = ?, error_message = ?
		WHERE id = ?
	`

	_, err := psm.db.Exec(query, status, durationMs, errorMsgPtr, executionID)
	return err
}

// logExecutionEvent logs a step execution event
func (psm *PipelineStateManager) logExecutionEvent(executionID int, event core.ChangeEvent) {
	var stepName *string
	if event.StepName != "" {
		stepName = &event.StepName
	}

	var data *string
	if event.Data != nil {
		// TODO: Serialize event.Data to JSON
		jsonData := "{}" // Placeholder
		data = &jsonData
	}

	level := "info"
	message := fmt.Sprintf("Step %s %s", event.StepName, event.Type)

	query := `
		INSERT INTO execution_logs (execution_id, step_name, level, message, data)
		VALUES (?, ?, ?, ?, ?)
	`

	psm.db.Exec(query, executionID, stepName, level, message, data)
}

