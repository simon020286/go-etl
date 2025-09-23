package db

import (
	"database/sql"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// PipelineManager handles CRUD operations for pipelines
type PipelineManager struct {
	db *sql.DB
}

// NewPipelineManager creates a new pipeline manager instance
func NewPipelineManager(db *sql.DB) *PipelineManager {
	return &PipelineManager{db: db}
}

// CreatePipelineRequest represents a request to create a new pipeline
type CreatePipelineRequest struct {
	Name        string  `json:"name" validate:"required"`
	Description string  `json:"description"`
	ConfigYAML  string  `json:"config_yaml" validate:"required"`
	Tags        []string `json:"tags,omitempty"`
	Enabled     *bool   `json:"enabled,omitempty"`
}

// UpdatePipelineRequest represents a request to update an existing pipeline
type UpdatePipelineRequest struct {
	Name        *string  `json:"name,omitempty"`
	Description *string  `json:"description,omitempty"`
	ConfigYAML  *string  `json:"config_yaml,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Enabled     *bool    `json:"enabled,omitempty"`
}

// ListPipelinesRequest represents filtering options for listing pipelines
type ListPipelinesRequest struct {
	State    string `json:"state,omitempty"`
	Enabled  *bool  `json:"enabled,omitempty"`
	Offset   int    `json:"offset"`
	Limit    int    `json:"limit"`
	OrderBy  string `json:"order_by"` // name, created_at, updated_at, last_run_at
	OrderDir string `json:"order_dir"` // asc, desc
}

// PipelineResponse represents a pipeline with additional computed fields
type PipelineResponse struct {
	Pipeline
	ExecutionCount int    `json:"execution_count"`
	LastStatus     string `json:"last_status,omitempty"`
}

// CreatePipeline creates a new pipeline
func (pm *PipelineManager) CreatePipeline(req CreatePipelineRequest) (*Pipeline, error) {
	// Validate YAML configuration
	if err := pm.validatePipelineConfig(req.ConfigYAML); err != nil {
		return nil, fmt.Errorf("invalid pipeline configuration: %w", err)
	}

	// Serialize tags
	var tagsJSON *string
	if len(req.Tags) > 0 {
		tags := fmt.Sprintf(`["%s"]`, req.Tags[0])
		for _, tag := range req.Tags[1:] {
			tags = tags[:len(tags)-1] + fmt.Sprintf(`, "%s"]`, tag)
		}
		tagsJSON = &tags
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	query := `
		INSERT INTO pipelines (name, description, config_yaml, state, enabled, tags)
		VALUES (?, ?, ?, ?, ?, ?)
		RETURNING id, name, description, config_yaml, state, created_at, updated_at,
		          last_run_at, next_run_at, enabled, schedule_cron, tags
	`

	var pipeline Pipeline
	err := pm.db.QueryRow(query, req.Name, req.Description, req.ConfigYAML, StateCreated, enabled, tagsJSON).Scan(
		&pipeline.ID, &pipeline.Name, &pipeline.Description, &pipeline.ConfigYAML,
		&pipeline.State, &pipeline.CreatedAt, &pipeline.UpdatedAt,
		&pipeline.LastRunAt, &pipeline.NextRunAt, &pipeline.Enabled,
		&pipeline.ScheduleCron, &pipeline.Tags,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create pipeline: %w", err)
	}

	return &pipeline, nil
}

// GetPipeline retrieves a pipeline by ID
func (pm *PipelineManager) GetPipeline(id int) (*Pipeline, error) {
	query := `
		SELECT id, name, description, config_yaml, state, created_at, updated_at,
		       last_run_at, next_run_at, enabled, schedule_cron, tags
		FROM pipelines
		WHERE id = ?
	`

	var pipeline Pipeline
	err := pm.db.QueryRow(query, id).Scan(
		&pipeline.ID, &pipeline.Name, &pipeline.Description, &pipeline.ConfigYAML,
		&pipeline.State, &pipeline.CreatedAt, &pipeline.UpdatedAt,
		&pipeline.LastRunAt, &pipeline.NextRunAt, &pipeline.Enabled,
		&pipeline.ScheduleCron, &pipeline.Tags,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("pipeline with id %d not found", id)
		}
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	return &pipeline, nil
}

// GetPipelineByName retrieves a pipeline by name
func (pm *PipelineManager) GetPipelineByName(name string) (*Pipeline, error) {
	query := `
		SELECT id, name, description, config_yaml, state, created_at, updated_at,
		       last_run_at, next_run_at, enabled, schedule_cron, tags
		FROM pipelines
		WHERE name = ?
	`

	var pipeline Pipeline
	err := pm.db.QueryRow(query, name).Scan(
		&pipeline.ID, &pipeline.Name, &pipeline.Description, &pipeline.ConfigYAML,
		&pipeline.State, &pipeline.CreatedAt, &pipeline.UpdatedAt,
		&pipeline.LastRunAt, &pipeline.NextRunAt, &pipeline.Enabled,
		&pipeline.ScheduleCron, &pipeline.Tags,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("pipeline with name '%s' not found", name)
		}
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	return &pipeline, nil
}

// UpdatePipeline updates an existing pipeline
func (pm *PipelineManager) UpdatePipeline(id int, req UpdatePipelineRequest) (*Pipeline, error) {
	// First, get the current pipeline to ensure it exists
	current, err := pm.GetPipeline(id)
	if err != nil {
		return nil, err
	}

	// Validate new configuration if provided
	if req.ConfigYAML != nil {
		if err := pm.validatePipelineConfig(*req.ConfigYAML); err != nil {
			return nil, fmt.Errorf("invalid pipeline configuration: %w", err)
		}
	}

	// Build update query dynamically
	setParts := []string{}
	args := []interface{}{}

	if req.Name != nil {
		setParts = append(setParts, "name = ?")
		args = append(args, *req.Name)
	}
	if req.Description != nil {
		setParts = append(setParts, "description = ?")
		args = append(args, *req.Description)
	}
	if req.ConfigYAML != nil {
		setParts = append(setParts, "config_yaml = ?")
		args = append(args, *req.ConfigYAML)
	}
	if req.Enabled != nil {
		setParts = append(setParts, "enabled = ?")
		args = append(args, *req.Enabled)
	}
	if len(req.Tags) > 0 {
		tags := fmt.Sprintf(`["%s"]`, req.Tags[0])
		for _, tag := range req.Tags[1:] {
			tags = tags[:len(tags)-1] + fmt.Sprintf(`, "%s"]`, tag)
		}
		setParts = append(setParts, "tags = ?")
		args = append(args, tags)
	}

	if len(setParts) == 0 {
		return current, nil // No updates requested
	}

	setParts = append(setParts, "updated_at = CURRENT_TIMESTAMP")
	args = append(args, id)

	setClause := setParts[0]
	for i := 1; i < len(setParts); i++ {
		setClause = fmt.Sprintf("%s, %s", setClause, setParts[i])
	}

	query := fmt.Sprintf(`
		UPDATE pipelines
		SET %s
		WHERE id = ?
	`, setClause)

	_, err = pm.db.Exec(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to update pipeline: %w", err)
	}

	// Return updated pipeline
	return pm.GetPipeline(id)
}

// DeletePipeline deletes a pipeline by ID
func (pm *PipelineManager) DeletePipeline(id int) error {
	// Check if pipeline exists
	_, err := pm.GetPipeline(id)
	if err != nil {
		return err
	}

	query := "DELETE FROM pipelines WHERE id = ?"
	result, err := pm.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to delete pipeline: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("pipeline with id %d not found", id)
	}

	return nil
}

// ListPipelines retrieves pipelines with filtering and pagination
func (pm *PipelineManager) ListPipelines(req ListPipelinesRequest) ([]PipelineResponse, int, error) {
	// Build WHERE clause
	whereClause := "WHERE 1=1"
	args := []interface{}{}

	if req.State != "" {
		whereClause += " AND state = ?"
		args = append(args, req.State)
	}
	if req.Enabled != nil {
		whereClause += " AND enabled = ?"
		args = append(args, *req.Enabled)
	}

	// Count total records
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM pipelines %s", whereClause)
	var total int
	err := pm.db.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count pipelines: %w", err)
	}

	// Build ORDER BY clause
	orderBy := "created_at"
	if req.OrderBy != "" {
		switch req.OrderBy {
		case "name", "created_at", "updated_at", "last_run_at":
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

	// Build main query with pipeline data and execution stats
	query := fmt.Sprintf(`
		SELECT p.id, p.name, p.description, p.config_yaml, p.state,
		       p.created_at, p.updated_at, p.last_run_at, p.next_run_at,
		       p.enabled, p.schedule_cron, p.tags,
		       COUNT(e.id) as execution_count,
		       COALESCE(latest.status, '') as last_status
		FROM pipelines p
		LEFT JOIN executions e ON p.id = e.pipeline_id
		LEFT JOIN (
			SELECT DISTINCT pipeline_id,
			       FIRST_VALUE(status) OVER (PARTITION BY pipeline_id ORDER BY started_at DESC) as status
			FROM executions
		) latest ON p.id = latest.pipeline_id
		%s
		GROUP BY p.id, p.name, p.description, p.config_yaml, p.state,
		         p.created_at, p.updated_at, p.last_run_at, p.next_run_at,
		         p.enabled, p.schedule_cron, p.tags, latest.status
		ORDER BY p.%s %s
		LIMIT ? OFFSET ?
	`, whereClause, orderBy, orderDir)

	args = append(args, limit, req.Offset)

	rows, err := pm.db.Query(query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []PipelineResponse
	for rows.Next() {
		var pr PipelineResponse
		err := rows.Scan(
			&pr.ID, &pr.Name, &pr.Description, &pr.ConfigYAML,
			&pr.State, &pr.CreatedAt, &pr.UpdatedAt,
			&pr.LastRunAt, &pr.NextRunAt, &pr.Enabled,
			&pr.ScheduleCron, &pr.Tags,
			&pr.ExecutionCount, &pr.LastStatus,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan pipeline: %w", err)
		}
		pipelines = append(pipelines, pr)
	}

	return pipelines, total, nil
}

// UpdatePipelineState updates the state of a pipeline
func (pm *PipelineManager) UpdatePipelineState(id int, state string) error {
	validStates := map[string]bool{
		StateCreated:   true,
		StateRunning:   true,
		StatePaused:    true,
		StateStopped:   true,
		StateError:     true,
		StateCompleted: true,
	}

	if !validStates[state] {
		return fmt.Errorf("invalid state: %s", state)
	}

	query := `
		UPDATE pipelines
		SET state = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`

	result, err := pm.db.Exec(query, state, id)
	if err != nil {
		return fmt.Errorf("failed to update pipeline state: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("pipeline with id %d not found", id)
	}

	return nil
}

// UpdatePipelineLastRun updates the last run timestamp for a pipeline
func (pm *PipelineManager) UpdatePipelineLastRun(id int, timestamp time.Time) error {
	query := `
		UPDATE pipelines
		SET last_run_at = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?
	`

	result, err := pm.db.Exec(query, timestamp, id)
	if err != nil {
		return fmt.Errorf("failed to update pipeline last run: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("pipeline with id %d not found", id)
	}

	return nil
}

// validatePipelineConfig validates that the YAML configuration is valid
func (pm *PipelineManager) validatePipelineConfig(configYAML string) error {
	var config map[string]interface{}
	err := yaml.Unmarshal([]byte(configYAML), &config)
	if err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}

	// Basic validation - check for required 'steps' field
	steps, exists := config["steps"]
	if !exists {
		return fmt.Errorf("configuration must contain 'steps' field")
	}

	stepsSlice, ok := steps.([]interface{})
	if !ok {
		return fmt.Errorf("'steps' field must be an array")
	}

	if len(stepsSlice) == 0 {
		return fmt.Errorf("pipeline must contain at least one step")
	}

	// Validate each step has required fields
	for i, step := range stepsSlice {
		stepMap, ok := step.(map[string]interface{})
		if !ok {
			return fmt.Errorf("step %d must be an object", i)
		}

		if _, exists := stepMap["name"]; !exists {
			return fmt.Errorf("step %d must have a 'name' field", i)
		}

		if _, exists := stepMap["type"]; !exists {
			return fmt.Errorf("step %d must have a 'type' field", i)
		}
	}

	return nil
}