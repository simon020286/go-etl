package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pipeline "github.com/simon020286/go-pipeline"
	"github.com/simon020286/go-pipeline/models"
	"github.com/simon020286/go-pipeline/pkg"
	_ "github.com/simon020286/go-pipeline/steps"
	"gopkg.in/yaml.v3"
)

// PipelineStateManager handles pipeline state transitions and execution tracking
type PipelineStateManager struct {
	db               *sql.DB
	pipelineManager  *PipelineManager
	runningPipelines map[int]*RunningPipeline
	mu               sync.RWMutex
	eventListeners   []StateEventListener
	stepListeners    []StepEventListener
}

// RunningPipeline tracks an active pipeline execution using the external go-pipeline library
type RunningPipeline struct {
	ID         int
	Pipeline   pipeline.IPipeline // Using interface from external library
	Execution  *Execution
	Context    context.Context
	CancelFunc context.CancelFunc
	StartTime  time.Time
	Status     string
	mu         sync.RWMutex
}

// StateEvent represents a pipeline state change event
type StateEvent struct {
	PipelineID   int                    `json:"pipeline_id"`
	PipelineName string                 `json:"pipeline_name"`
	OldState     string                 `json:"old_state"`
	NewState     string                 `json:"new_state"`
	Timestamp    time.Time              `json:"timestamp"`
	ExecutionID  *int                   `json:"execution_id,omitempty"`
	Data         map[string]interface{} `json:"data,omitempty"`
	Error        string                 `json:"error,omitempty"`
}

// StepExecutionEvent represents a step-level execution event
type StepExecutionEvent struct {
	PipelineID   int                    `json:"pipeline_id"`
	PipelineName string                 `json:"pipeline_name"`
	ExecutionID  int                    `json:"execution_id"`
	StepName     string                 `json:"step_name"`
	Status       string                 `json:"status"` // "running", "completed", "error"
	Timestamp    time.Time              `json:"timestamp"`
	Data         map[string]interface{} `json:"data,omitempty"`
	Error        string                 `json:"error,omitempty"`
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

// StepEventListener defines the interface for step-level event listeners
type StepEventListener interface {
	OnStepEvent(event StepExecutionEvent)
}

// StepEventListenerFunc is a function adapter for StepEventListener
type StepEventListenerFunc func(event StepExecutionEvent)

func (f StepEventListenerFunc) OnStepEvent(event StepExecutionEvent) {
	f(event)
}

// NewPipelineStateManager creates a new pipeline state manager
func NewPipelineStateManager(db *sql.DB, pipelineManager *PipelineManager) *PipelineStateManager {
	return &PipelineStateManager{
		db:               db,
		pipelineManager:  pipelineManager,
		runningPipelines: make(map[int]*RunningPipeline),
		eventListeners:   make([]StateEventListener, 0),
		stepListeners:    make([]StepEventListener, 0),
	}
}

// AddStateListener adds a listener for state change events
func (psm *PipelineStateManager) AddStateListener(listener StateEventListener) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	psm.eventListeners = append(psm.eventListeners, listener)
}

// AddStepListener adds a listener for step execution events
func (psm *PipelineStateManager) AddStepListener(listener StepEventListener) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	psm.stepListeners = append(psm.stepListeners, listener)
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

// emitStepEvent emits a step execution event to all listeners
func (psm *PipelineStateManager) emitStepEvent(event StepExecutionEvent) {
	psm.mu.RLock()
	stepListeners := make([]StepEventListener, len(psm.stepListeners))
	copy(stepListeners, psm.stepListeners)
	psm.mu.RUnlock()

	for _, listener := range stepListeners {
		go listener.OnStepEvent(event)
	}
}

// StartPipeline starts a pipeline execution using the external go-pipeline library
func (psm *PipelineStateManager) StartPipeline(pipelineID int, triggerType, triggerData string) (*Execution, error) {
	slog.Debug("StartPipeline: Starting pipeline", "pipeline_id", pipelineID, "trigger", triggerType)

	// Check if pipeline is already running
	psm.mu.RLock()
	if _, isRunning := psm.runningPipelines[pipelineID]; isRunning {
		psm.mu.RUnlock()
		return nil, fmt.Errorf("pipeline %d is already running", pipelineID)
	}
	psm.mu.RUnlock()

	// Get pipeline from database
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	if !pipelineRecord.Enabled {
		return nil, fmt.Errorf("pipeline %d is disabled", pipelineID)
	}

	// Parse YAML configuration using the external library
	var config pkg.PipelineConfig
	if err := yaml.Unmarshal([]byte(pipelineRecord.ConfigYAML), &config); err != nil {
		return nil, fmt.Errorf("failed to parse pipeline YAML: %w", err)
	}

	// Build pipeline from config using the external library
	pipelineInstance, err := pipeline.BuildFromConfig(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to build pipeline: %w", err)
	}

	// Create execution record
	execution, err := psm.createExecution(pipelineID, triggerType, triggerData)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution record: %w", err)
	}

	// Update pipeline state to RUNNING
	oldState := pipelineRecord.State
	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StateRunning)
	if err != nil {
		psm.updateExecution(execution.ID, StateError, nil, err.Error())
		return nil, fmt.Errorf("failed to update pipeline state: %w", err)
	}

	// Create running pipeline context
	ctx, cancel := context.WithCancel(context.Background())
	runningPipeline := &RunningPipeline{
		ID:         pipelineID,
		Pipeline:   pipelineInstance,
		Execution:  execution,
		Context:    ctx,
		CancelFunc: cancel,
		StartTime:  time.Now(),
		Status:     StateRunning,
	}

	// Add event listener to pipeline for execution logging
	eventLogger := &pipelineExecutionLogger{
		psm:          psm,
		pipelineID:   pipelineID,
		pipelineName: pipelineRecord.Name,
		executionID:  execution.ID,
	}
	pipelineInstance.AddListener(eventLogger)

	// Register running pipeline
	psm.mu.Lock()
	psm.runningPipelines[pipelineID] = runningPipeline
	psm.mu.Unlock()

	// Emit state change event
	psm.emitStateEvent(StateEvent{
		PipelineID:   pipelineID,
		PipelineName: pipelineRecord.Name,
		OldState:     oldState,
		NewState:     StateRunning,
		Timestamp:    time.Now(),
		ExecutionID:  &execution.ID,
	})

	// Start pipeline execution in goroutine
	go psm.executePipeline(runningPipeline, pipelineRecord)

	return execution, nil
}

// StopPipeline stops a running pipeline
func (psm *PipelineStateManager) StopPipeline(pipelineID int) error {
	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return fmt.Errorf("failed to get pipeline: %w", err)
	}

	psm.mu.RLock()
	runningPipeline, exists := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if exists {
		// Cancel the pipeline context
		runningPipeline.CancelFunc()

		// Update in-memory status
		runningPipeline.mu.Lock()
		runningPipeline.Status = StateStopped
		runningPipeline.mu.Unlock()
	} else if pipelineRecord.State != StateRunning && pipelineRecord.State != StatePaused {
		return fmt.Errorf("pipeline %d is not running (current state: %s)", pipelineID, pipelineRecord.State)
	}

	// Update database state
	oldState := pipelineRecord.State
	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StateStopped)
	if err != nil {
		return fmt.Errorf("failed to update pipeline state: %w", err)
	}

	// Emit state change event
	psm.emitStateEvent(StateEvent{
		PipelineID:   pipelineID,
		PipelineName: pipelineRecord.Name,
		OldState:     oldState,
		NewState:     StateStopped,
		Timestamp:    time.Now(),
	})

	slog.Info("Pipeline stopped", "pipeline_id", pipelineID)
	return nil
}

// PausePipeline pauses a running pipeline
func (psm *PipelineStateManager) PausePipeline(pipelineID int) error {
	psm.mu.RLock()
	runningPipeline, exists := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("pipeline %d is not running", pipelineID)
	}

	runningPipeline.mu.Lock()
	runningPipeline.Status = StatePaused
	runningPipeline.mu.Unlock()

	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return err
	}

	oldState := pipelineRecord.State
	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StatePaused)
	if err != nil {
		return err
	}

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

	runningPipeline.mu.Lock()
	runningPipeline.Status = StateRunning
	runningPipeline.mu.Unlock()

	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return err
	}

	err = psm.pipelineManager.UpdatePipelineState(pipelineID, StateRunning)
	if err != nil {
		return err
	}

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
	psm.mu.RLock()
	runningPipeline, isRunning := psm.runningPipelines[pipelineID]
	psm.mu.RUnlock()

	if isRunning {
		runningPipeline.mu.RLock()
		status := runningPipeline.Status
		runningPipeline.mu.RUnlock()
		return status, nil
	}

	pipelineRecord, err := psm.pipelineManager.GetPipeline(pipelineID)
	if err != nil {
		return "", err
	}

	return pipelineRecord.State, nil
}

// executePipeline runs the actual pipeline execution using the external library
func (psm *PipelineStateManager) executePipeline(runningPipeline *RunningPipeline, pipelineRecord *Pipeline) {
	defer func() {
		// Clean up running pipeline from memory
		psm.mu.Lock()
		delete(psm.runningPipelines, runningPipeline.ID)
		psm.mu.Unlock()
	}()

	var finalState string
	var errorMsg string

	// Start the pipeline using the external library
	err := runningPipeline.Pipeline.Start(runningPipeline.Context)
	if err != nil {
		finalState = StateError
		errorMsg = err.Error()
		slog.Error("Pipeline start failed", "pipeline_id", runningPipeline.ID, "error", err)
	} else {
		// Wait for pipeline to complete
		runningPipeline.Pipeline.Wait()

		// Check if context was cancelled (stopped by user)
		if runningPipeline.Context.Err() != nil {
			finalState = StateStopped
		} else {
			finalState = StateCompleted
		}
	}

	duration := time.Since(runningPipeline.StartTime)
	durationMs := int(duration.Milliseconds())

	// Update execution record
	psm.updateExecution(runningPipeline.Execution.ID, finalState, &durationMs, errorMsg)

	// Update pipeline state in database
	err = psm.pipelineManager.UpdatePipelineState(runningPipeline.ID, finalState)
	if err != nil {
		slog.Error("Failed to update pipeline state", "state", finalState, "error", err)
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

	slog.Info("Pipeline execution completed", "pipeline_id", runningPipeline.ID, "state", finalState, "duration", duration)
}

// createExecution creates a new execution record
func (psm *PipelineStateManager) createExecution(pipelineID int, triggerType, triggerData string) (*Execution, error) {
	var triggerDataPtr *string
	if triggerData != "" {
		triggerDataPtr = &triggerData
	}

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

// RestoreRunningPipelines restarts all pipelines that were RUNNING when the server stopped
func (psm *PipelineStateManager) RestoreRunningPipelines() error {
	slog.Info("Restoring running pipelines from database")

	query := `SELECT id, name, state FROM pipelines WHERE state IN (?, ?)`
	rows, err := psm.db.Query(query, StateRunning, StatePaused)
	if err != nil {
		return fmt.Errorf("failed to query running pipelines: %w", err)
	}
	defer rows.Close()

	var pipelinesToRestore []struct {
		ID    int
		Name  string
		State string
	}

	for rows.Next() {
		var p struct {
			ID    int
			Name  string
			State string
		}
		if err := rows.Scan(&p.ID, &p.Name, &p.State); err != nil {
			slog.Error("Failed to scan pipeline row", "error", err)
			continue
		}
		pipelinesToRestore = append(pipelinesToRestore, p)
	}

	if len(pipelinesToRestore) == 0 {
		slog.Info("No pipelines to restore")
		return nil
	}

	slog.Info("Found pipelines to restore", "count", len(pipelinesToRestore))

	// Restart each pipeline
	for _, p := range pipelinesToRestore {
		slog.Info("Restoring pipeline", "id", p.ID, "name", p.Name, "state", p.State)

		// Reset state to CREATED to allow StartPipeline to work
		err := psm.pipelineManager.UpdatePipelineState(p.ID, StateCreated)
		if err != nil {
			slog.Error("Failed to reset pipeline state", "id", p.ID, "error", err)
			continue
		}

		// Start the pipeline
		_, err = psm.StartPipeline(p.ID, "system", "restored_on_startup")
		if err != nil {
			slog.Error("Failed to restore pipeline", "id", p.ID, "name", p.Name, "error", err)
			continue
		}

		slog.Info("Pipeline restored successfully", "id", p.ID, "name", p.Name)
	}

	return nil
}

// logExecutionEvent logs an execution event to the database
func (psm *PipelineStateManager) logExecutionEvent(executionID int, stepName, level, message string, data interface{}) {
	var stepNamePtr *string
	if stepName != "" {
		stepNamePtr = &stepName
	}

	var dataPtr *string
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err == nil {
			dataStr := string(jsonData)
			dataPtr = &dataStr
		}
	}

	query := `
		INSERT INTO execution_logs (execution_id, step_name, level, message, data)
		VALUES (?, ?, ?, ?, ?)
	`

	_, err := psm.db.Exec(query, executionID, stepNamePtr, level, message, dataPtr)
	if err != nil {
		slog.Error("Failed to log execution event", "error", err)
	}
}

// pipelineExecutionLogger implements the EventListener interface from the external library
type pipelineExecutionLogger struct {
	psm          *PipelineStateManager
	pipelineID   int
	pipelineName string
	executionID  int
}

func (l *pipelineExecutionLogger) OnEvent(event models.Event) {
	switch event.Type {
	case models.EventPipelineStarted:
		mode := "unknown"
		if m, ok := event.Data["mode"].(string); ok {
			mode = m
		}
		l.psm.logExecutionEvent(l.executionID, "", "info", fmt.Sprintf("Pipeline started in %s mode", mode), nil)

	case models.EventPipelineCompleted:
		duration := time.Duration(0)
		if d, ok := event.Data["duration"].(time.Duration); ok {
			duration = d
		}
		l.psm.logExecutionEvent(l.executionID, "", "info", fmt.Sprintf("Pipeline completed in %v", duration), nil)

	case models.EventPipelineError:
		errMsg := "unknown error"
		if e, ok := event.Data["error"].(string); ok {
			errMsg = e
		}
		l.psm.logExecutionEvent(l.executionID, "", "error", fmt.Sprintf("Pipeline error: %s", errMsg), nil)

	case models.EventStageOutput:
		stageID := "unknown"
		eventID := "unknown"
		if s, ok := event.Data["stage_id"].(string); ok {
			stageID = s
		}
		if e, ok := event.Data["event_id"].(string); ok {
			eventID = e
		}

		output := event.Data["output"]
		l.psm.logExecutionEvent(l.executionID, stageID, "info", fmt.Sprintf("Stage output (event: %s)", eventID), output)

		// Emit step execution event for WebSocket clients
		l.psm.emitStepEvent(StepExecutionEvent{
			PipelineID:   l.pipelineID,
			PipelineName: l.pipelineName,
			ExecutionID:  l.executionID,
			StepName:     stageID,
			Status:       "completed",
			Timestamp:    event.Timestamp,
			Data:         map[string]interface{}{"output": output},
		})

	case models.EventStageError:
		stageID := "unknown"
		eventID := "unknown"
		errMsg := "unknown error"
		if s, ok := event.Data["stage_id"].(string); ok {
			stageID = s
		}
		if e, ok := event.Data["event_id"].(string); ok {
			eventID = e
		}
		if err, ok := event.Data["error"].(string); ok {
			errMsg = err
		}

		l.psm.logExecutionEvent(l.executionID, stageID, "error", fmt.Sprintf("Stage error (event: %s): %s", eventID, errMsg), nil)

		// Emit step execution event for WebSocket clients
		l.psm.emitStepEvent(StepExecutionEvent{
			PipelineID:   l.pipelineID,
			PipelineName: l.pipelineName,
			ExecutionID:  l.executionID,
			StepName:     stageID,
			Status:       "error",
			Timestamp:    event.Timestamp,
			Error:        errMsg,
		})
	}
}
