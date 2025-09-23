package db

import (
	"database/sql"
	"fmt"
)

// ExecutionManager handles execution CRUD operations
type ExecutionManager struct {
	db *sql.DB
}

// NewExecutionManager creates a new execution manager
func NewExecutionManager(db *sql.DB) *ExecutionManager {
	return &ExecutionManager{db: db}
}

// ListExecutionsRequest represents filtering options for listing executions
type ListExecutionsRequest struct {
	PipelineID int    `json:"pipeline_id,omitempty"`
	Status     string `json:"status,omitempty"`
	Offset     int    `json:"offset"`
	Limit      int    `json:"limit"`
	OrderBy    string `json:"order_by"` // started_at, completed_at, duration_ms
	OrderDir   string `json:"order_dir"` // asc, desc
}

// GetExecution retrieves an execution by ID
func (em *ExecutionManager) GetExecution(id int) (*Execution, error) {
	query := `
		SELECT id, pipeline_id, status, started_at, completed_at, duration_ms,
		       error_message, trigger_type, trigger_data
		FROM executions
		WHERE id = ?
	`

	var execution Execution
	err := em.db.QueryRow(query, id).Scan(
		&execution.ID, &execution.PipelineID, &execution.Status,
		&execution.StartedAt, &execution.CompletedAt, &execution.DurationMs,
		&execution.ErrorMessage, &execution.TriggerType, &execution.TriggerData,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("execution with id %d not found", id)
		}
		return nil, fmt.Errorf("failed to get execution: %w", err)
	}

	return &execution, nil
}

// ListExecutions retrieves executions with filtering and pagination
func (em *ExecutionManager) ListExecutions(req ListExecutionsRequest) ([]Execution, int, error) {
	// Build WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	if req.PipelineID > 0 {
		whereClause += " AND pipeline_id = ?"
		args = append(args, req.PipelineID)
	}
	if req.Status != "" {
		whereClause += " AND status = ?"
		args = append(args, req.Status)
	}

	// Count total records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM executions %s", whereClause)
	var total int
	err := em.db.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count executions: %w", err)
	}

	// Build ORDER BY clause
	orderBy := "started_at"
	if req.OrderBy != "" {
		switch req.OrderBy {
		case "started_at", "completed_at", "duration_ms":
			orderBy = req.OrderBy
		}
	}

	orderDir := "DESC"
	if req.OrderDir == "asc" {
		orderDir = "ASC"
	}

	// Set default limit
	limit := req.Limit
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	// Build main query
	query := fmt.Sprintf(`
		SELECT id, pipeline_id, status, started_at, completed_at, duration_ms,
		       error_message, trigger_type, trigger_data
		FROM executions
		%s
		ORDER BY %s %s
		LIMIT ? OFFSET ?
	`, whereClause, orderBy, orderDir)

	args = append(args, limit, req.Offset)

	rows, err := em.db.Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list executions: %w", err)
	}
	defer rows.Close()

	var executions []Execution
	for rows.Next() {
		var exec Execution
		err := rows.Scan(
			&exec.ID, &exec.PipelineID, &exec.Status,
			&exec.StartedAt, &exec.CompletedAt, &exec.DurationMs,
			&exec.ErrorMessage, &exec.TriggerType, &exec.TriggerData,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan execution: %w", err)
		}
		executions = append(executions, exec)
	}

	return executions, total, nil
}

// GetExecutionLogs retrieves logs for an execution
func (em *ExecutionManager) GetExecutionLogs(executionID int, limit int) ([]ExecutionLog, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	query := `
		SELECT id, execution_id, step_name, level, message, data, timestamp
		FROM execution_logs
		WHERE execution_id = ?
		ORDER BY timestamp ASC
		LIMIT ?
	`

	rows, err := em.db.Query(query, executionID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get execution logs: %w", err)
	}
	defer rows.Close()

	var logs []ExecutionLog
	for rows.Next() {
		var log ExecutionLog
		err := rows.Scan(
			&log.ID, &log.ExecutionID, &log.StepName,
			&log.Level, &log.Message, &log.Data, &log.Timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan execution log: %w", err)
		}
		logs = append(logs, log)
	}

	return logs, nil
}

// DeleteExecution deletes an execution and its logs
func (em *ExecutionManager) DeleteExecution(id int) error {
	// Check if execution exists
	_, err := em.GetExecution(id)
	if err != nil {
		return err
	}

	// Delete execution (logs will be deleted by CASCADE)
	query := "DELETE FROM executions WHERE id = ?"
	result, err := em.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete execution: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("execution with id %d not found", id)
	}

	return nil
}

// GetExecutionStats returns execution statistics for a pipeline
func (em *ExecutionManager) GetExecutionStats(pipelineID int) (*ExecutionStats, error) {
	query := `
		SELECT
			COUNT(*) as total_executions,
			COUNT(CASE WHEN status = ? THEN 1 END) as completed_executions,
			COUNT(CASE WHEN status = ? THEN 1 END) as error_executions,
			COUNT(CASE WHEN status = ? THEN 1 END) as running_executions,
			AVG(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as avg_duration_ms,
			MAX(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as max_duration_ms,
			MIN(CASE WHEN duration_ms IS NOT NULL THEN duration_ms END) as min_duration_ms
		FROM executions
		WHERE pipeline_id = ?
	`

	var stats ExecutionStats
	var avgDuration, maxDuration, minDuration sql.NullFloat64

	err := em.db.QueryRow(query, StateCompleted, StateError, StateRunning, pipelineID).Scan(
		&stats.TotalExecutions, &stats.CompletedExecutions, &stats.ErrorExecutions,
		&stats.RunningExecutions, &avgDuration, &maxDuration, &minDuration,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get execution stats: %w", err)
	}

	if avgDuration.Valid {
		avg := int(avgDuration.Float64)
		stats.AvgDurationMs = &avg
	}
	if maxDuration.Valid {
		max := int(maxDuration.Float64)
		stats.MaxDurationMs = &max
	}
	if minDuration.Valid {
		min := int(minDuration.Float64)
		stats.MinDurationMs = &min
	}

	return &stats, nil
}

// ExecutionStats represents execution statistics for a pipeline
type ExecutionStats struct {
	TotalExecutions     int `json:"total_executions"`
	CompletedExecutions int `json:"completed_executions"`
	ErrorExecutions     int `json:"error_executions"`
	RunningExecutions   int `json:"running_executions"`
	AvgDurationMs       *int `json:"avg_duration_ms,omitempty"`
	MaxDurationMs       *int `json:"max_duration_ms,omitempty"`
	MinDurationMs       *int `json:"min_duration_ms,omitempty"`
}