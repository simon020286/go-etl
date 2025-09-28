package web

import (
	"encoding/json"
	"fmt"
	"go-etl/db"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// handleListPipelines handles GET /api/v1/pipelines
func (s *APIServer) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	offset := s.parseQueryInt(r, "offset", 0)
	limit := s.parseQueryInt(r, "limit", 20)
	state := r.URL.Query().Get("state")
	enabled := s.parseQueryBool(r, "enabled")
	orderBy := r.URL.Query().Get("order_by")
	orderDir := r.URL.Query().Get("order_dir")

	// Validate order direction
	if orderDir != "" && orderDir != "asc" && orderDir != "desc" {
		s.sendError(w, http.StatusBadRequest, "order_dir must be 'asc' or 'desc'")
		return
	}

	// Create request
	req := db.ListPipelinesRequest{
		State:    state,
		Enabled:  enabled,
		Offset:   offset,
		Limit:    limit,
		OrderBy:  orderBy,
		OrderDir: orderDir,
	}

	// Get pipelines
	pipelines, total, err := s.manager.Pipelines().ListPipelines(req)
	if err != nil {
		s.logger.Error("Failed to list pipelines", "error", err)
		s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipelines")
		return
	}

	s.sendPaginated(w, pipelines, total, offset, limit)
}

// handleCreatePipeline handles POST /api/v1/pipelines
func (s *APIServer) handleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req db.CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	// Validate required fields
	if req.Name == "" {
		s.sendError(w, http.StatusBadRequest, "Name is required")
		return
	}

	if req.ConfigYAML == "" {
		s.sendError(w, http.StatusBadRequest, "ConfigYAML is required")
		return
	}

	// Create pipeline
	pipeline, err := s.manager.Pipelines().CreatePipeline(req)
	if err != nil {
		s.logger.Error("Failed to create pipeline", "error", err, "name", req.Name)
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			s.sendError(w, http.StatusConflict, "Pipeline with this name already exists")
		} else if strings.Contains(err.Error(), "invalid") {
			s.sendError(w, http.StatusBadRequest, err.Error())
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to create pipeline")
		}
		return
	}

	s.logger.Info("Pipeline created", "id", pipeline.ID, "name", pipeline.Name)

	// Broadcast event
	s.BroadcastMessage("pipeline_created", map[string]interface{}{
		"pipeline": pipeline,
	})

	s.sendSuccess(w, pipeline, "Pipeline created successfully")
}

// handleGetPipeline handles GET /api/v1/pipelines/{id}
func (s *APIServer) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	s.sendSuccess(w, pipeline)
}

// handleUpdatePipeline handles PUT /api/v1/pipelines/{id}
func (s *APIServer) handleUpdatePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req db.UpdatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	// Update pipeline
	pipeline, err := s.manager.Pipelines().UpdatePipeline(id, req)
	if err != nil {
		s.logger.Error("Failed to update pipeline", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			s.sendError(w, http.StatusConflict, "Pipeline with this name already exists")
		} else if strings.Contains(err.Error(), "invalid") {
			s.sendError(w, http.StatusBadRequest, err.Error())
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to update pipeline")
		}
		return
	}

	s.logger.Info("Pipeline updated", "id", pipeline.ID, "name", pipeline.Name)

	// Broadcast event
	s.BroadcastMessage("pipeline_updated", map[string]interface{}{
		"pipeline": pipeline,
	})

	s.sendSuccess(w, pipeline, "Pipeline updated successfully")
}

// handleDeletePipeline handles DELETE /api/v1/pipelines/{id}
func (s *APIServer) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Get pipeline info before deletion for logging
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Check if pipeline is running
	if s.manager.PipelineState().IsRunning(id) {
		s.sendError(w, http.StatusConflict, "Cannot delete running pipeline. Stop it first.")
		return
	}

	// Delete pipeline
	err = s.manager.Pipelines().DeletePipeline(id)
	if err != nil {
		s.logger.Error("Failed to delete pipeline", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to delete pipeline")
		return
	}

	s.logger.Info("Pipeline deleted", "id", id, "name", pipeline.Name)

	// Broadcast event
	s.BroadcastMessage("pipeline_deleted", map[string]interface{}{
		"pipeline_id": id,
		"pipeline_name": pipeline.Name,
	})

	s.sendSuccess(w, nil, "Pipeline deleted successfully")
}

// handleGetPipelineStatus handles GET /api/v1/pipelines/{id}/status
func (s *APIServer) handleGetPipelineStatus(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check if pipeline exists first
	_, err = s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for status", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Get status from state manager
	status, err := s.manager.PipelineState().GetPipelineStatus(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline status", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline status")
		return
	}

	// Check if running
	isRunning := s.manager.PipelineState().IsRunning(id)

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id": id,
		"status":      status,
		"is_running":  isRunning,
	})
}

// Pipeline control handlers
func (s *APIServer) handleStartPipeline(w http.ResponseWriter, r *http.Request) {
	fmt.Printf("[DEBUG] handleStartPipeline: Handler called\n")

	id, err := s.extractID(r)
	if err != nil {
		fmt.Printf("[DEBUG] handleStartPipeline: Failed to extract ID: %v\n", err)
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}
	fmt.Printf("[DEBUG] handleStartPipeline: Extracted pipeline ID: %d\n", id)

	// Parse optional request body for trigger data
	type StartRequest struct {
		TriggerType string `json:"trigger_type,omitempty"`
		TriggerData string `json:"trigger_data,omitempty"`
	}

	var req StartRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req) // Ignore errors for optional body
	}

	// Set defaults
	if req.TriggerType == "" {
		req.TriggerType = "manual"
	}

	// Check if pipeline exists and get its details
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for start", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Check if pipeline is already running
	if s.manager.PipelineState().IsRunning(id) {
		s.sendError(w, http.StatusConflict, "Pipeline is already running")
		return
	}

	// Check if pipeline is enabled
	if !pipeline.Enabled {
		s.sendError(w, http.StatusForbidden, "Pipeline is disabled")
		return
	}

	// Start pipeline
	execution, err := s.manager.PipelineState().StartPipeline(id, req.TriggerType, req.TriggerData)
	if err != nil {
		s.logger.Error("Failed to start pipeline", "error", err, "id", id)
		if strings.Contains(err.Error(), "disabled") {
			s.sendError(w, http.StatusForbidden, "Pipeline is disabled")
		} else if strings.Contains(err.Error(), "already running") {
			s.sendError(w, http.StatusConflict, err.Error())
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to start pipeline execution")
		}
		return
	}

	s.logger.Info("Pipeline started", "id", id, "execution_id", execution.ID, "trigger", req.TriggerType)

	// Broadcast event
	s.BroadcastMessage("pipeline_started", map[string]interface{}{
		"pipeline_id":   id,
		"pipeline_name": pipeline.Name,
		"execution_id":  execution.ID,
		"trigger_type":  req.TriggerType,
	})

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id":  id,
		"execution":    execution,
		"message":      "Pipeline started successfully",
	})
}

func (s *APIServer) handleStopPipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check if pipeline exists
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for stop", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Check if running
	if !s.manager.PipelineState().IsRunning(id) {
		s.sendError(w, http.StatusConflict, "Pipeline is not running")
		return
	}

	// Stop pipeline
	err = s.manager.PipelineState().StopPipeline(id)
	if err != nil {
		s.logger.Error("Failed to stop pipeline", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to stop pipeline execution")
		return
	}

	s.logger.Info("Pipeline stopped", "id", id)

	// Broadcast event
	s.BroadcastMessage("pipeline_stopped", map[string]interface{}{
		"pipeline_id":   id,
		"pipeline_name": pipeline.Name,
	})

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id": id,
		"message":     "Pipeline stopped successfully",
	})
}

func (s *APIServer) handlePausePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check if pipeline exists
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for pause", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Check if running
	if !s.manager.PipelineState().IsRunning(id) {
		s.sendError(w, http.StatusConflict, "Pipeline is not running")
		return
	}

	// Get current status
	status, err := s.manager.PipelineState().GetPipelineStatus(id)
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, "Failed to get pipeline status")
		return
	}

	if status == "PAUSED" {
		s.sendError(w, http.StatusConflict, "Pipeline is already paused")
		return
	}

	// Pause pipeline
	err = s.manager.PipelineState().PausePipeline(id)
	if err != nil {
		s.logger.Error("Failed to pause pipeline", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to pause pipeline execution")
		return
	}

	s.logger.Info("Pipeline paused", "id", id)

	// Broadcast event
	s.BroadcastMessage("pipeline_paused", map[string]interface{}{
		"pipeline_id":   id,
		"pipeline_name": pipeline.Name,
	})

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id": id,
		"message":     "Pipeline paused successfully",
	})
}

func (s *APIServer) handleResumePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Check if pipeline exists
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for resume", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Check if running
	if !s.manager.PipelineState().IsRunning(id) {
		s.sendError(w, http.StatusConflict, "Pipeline is not running")
		return
	}

	// Get current status
	status, err := s.manager.PipelineState().GetPipelineStatus(id)
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, "Failed to get pipeline status")
		return
	}

	if status != "PAUSED" {
		s.sendError(w, http.StatusConflict, "Pipeline is not paused")
		return
	}

	// Resume pipeline
	err = s.manager.PipelineState().ResumePipeline(id)
	if err != nil {
		s.logger.Error("Failed to resume pipeline", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to resume pipeline execution")
		return
	}

	s.logger.Info("Pipeline resumed", "id", id)

	// Broadcast event
	s.BroadcastMessage("pipeline_resumed", map[string]interface{}{
		"pipeline_id":   id,
		"pipeline_name": pipeline.Name,
	})

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id": id,
		"message":     "Pipeline resumed successfully",
	})
}

// handleGetRunningPipelines handles GET /api/v1/pipelines/running
func (s *APIServer) handleGetRunningPipelines(w http.ResponseWriter, r *http.Request) {
	runningPipelines := s.manager.PipelineState().GetRunningPipelines()

	// Convert to a more detailed response
	var result []map[string]interface{}
	for id, rp := range runningPipelines {
		// Get pipeline details
		pipeline, err := s.manager.Pipelines().GetPipeline(id)
		if err != nil {
			s.logger.Error("Failed to get pipeline details for running pipeline", "error", err, "id", id)
			continue
		}

		// Get current status safely
		status, err := s.manager.PipelineState().GetPipelineStatus(id)
		if err != nil {
			s.logger.Error("Failed to get pipeline status for running pipeline", "error", err, "id", id)
			status = "UNKNOWN"
		}

		runningInfo := map[string]interface{}{
			"pipeline_id":   id,
			"pipeline_name": pipeline.Name,
			"execution_id":  rp.Execution.ID,
			"status":        status,
			"started_at":    rp.StartTime,
			"duration_ms":   time.Since(rp.StartTime).Milliseconds(),
		}
		result = append(result, runningInfo)
	}

	s.sendSuccess(w, result)
}

// handleUploadPipeline handles POST /api/v1/pipelines/upload
func (s *APIServer) handleUploadPipeline(w http.ResponseWriter, r *http.Request) {
	// Parse multipart form
	err := r.ParseMultipartForm(10 << 20) // 10 MB max
	if err != nil {
		s.sendError(w, http.StatusBadRequest, "Failed to parse multipart form")
		return
	}

	// Get file from form
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		s.sendError(w, http.StatusBadRequest, "No file provided")
		return
	}
	defer file.Close()

	// Validate file extension
	filename := fileHeader.Filename
	if !strings.HasSuffix(filename, ".yml") && !strings.HasSuffix(filename, ".yaml") && !strings.HasSuffix(filename, ".json") {
		s.sendError(w, http.StatusBadRequest, "File must be YAML (.yml, .yaml) or JSON (.json)")
		return
	}

	// Read file content
	content := make([]byte, fileHeader.Size)
	_, err = file.Read(content)
	if err != nil {
		s.sendError(w, http.StatusInternalServerError, "Failed to read file content")
		return
	}

	// Get optional form fields
	name := r.FormValue("name")
	description := r.FormValue("description")
	enabledStr := r.FormValue("enabled")

	// If name not provided, use filename without extension
	if name == "" {
		name = strings.TrimSuffix(filename, filepath.Ext(filename))
	}

	// Parse enabled flag
	enabled := true
	if enabledStr != "" {
		enabled, _ = strconv.ParseBool(enabledStr)
	}

	// Validate YAML/JSON content
	configYAML := string(content)
	if strings.HasSuffix(filename, ".json") {
		// Convert JSON to YAML for storage
		var jsonData interface{}
		if err := json.Unmarshal(content, &jsonData); err != nil {
			s.sendError(w, http.StatusBadRequest, "Invalid JSON format: "+err.Error())
			return
		}

		yamlBytes, err := yaml.Marshal(jsonData)
		if err != nil {
			s.sendError(w, http.StatusInternalServerError, "Failed to convert JSON to YAML")
			return
		}
		configYAML = string(yamlBytes)
	} else {
		// Validate YAML
		var yamlData interface{}
		if err := yaml.Unmarshal(content, &yamlData); err != nil {
			s.sendError(w, http.StatusBadRequest, "Invalid YAML format: "+err.Error())
			return
		}
	}

	// Create pipeline request
	req := db.CreatePipelineRequest{
		Name:        name,
		Description: description,
		ConfigYAML:  configYAML,
		Enabled:     &enabled,
	}

	// Create pipeline
	pipeline, err := s.manager.Pipelines().CreatePipeline(req)
	if err != nil {
		s.logger.Error("Failed to create pipeline from upload", "error", err, "name", name)
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			s.sendError(w, http.StatusConflict, "Pipeline with this name already exists")
		} else if strings.Contains(err.Error(), "invalid") {
			s.sendError(w, http.StatusBadRequest, err.Error())
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to create pipeline")
		}
		return
	}

	s.logger.Info("Pipeline created from upload", "id", pipeline.ID, "name", pipeline.Name, "filename", filename)

	// Broadcast event
	s.BroadcastMessage("pipeline_uploaded", map[string]interface{}{
		"pipeline": pipeline,
		"filename": filename,
	})

	s.sendSuccess(w, pipeline, "Pipeline uploaded and created successfully")
}

// handleDownloadPipeline handles GET /api/v1/pipelines/{id}/download
func (s *APIServer) handleDownloadPipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Get pipeline
	pipeline, err := s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline for download", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Get format from query parameter (default: yaml)
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "yaml"
	}

	var content []byte
	var filename string
	var contentType string

	switch format {
	case "json":
		// Convert YAML to JSON
		var yamlData interface{}
		if err := yaml.Unmarshal([]byte(pipeline.ConfigYAML), &yamlData); err != nil {
			s.sendError(w, http.StatusInternalServerError, "Failed to parse pipeline configuration")
			return
		}

		content, err = json.MarshalIndent(yamlData, "", "  ")
		if err != nil {
			s.sendError(w, http.StatusInternalServerError, "Failed to convert to JSON")
			return
		}

		filename = fmt.Sprintf("%s.json", pipeline.Name)
		contentType = "application/json"

	case "yaml", "yml":
		content = []byte(pipeline.ConfigYAML)
		filename = fmt.Sprintf("%s.yml", pipeline.Name)
		contentType = "application/x-yaml"

	default:
		s.sendError(w, http.StatusBadRequest, "Invalid format. Supported: yaml, yml, json")
		return
	}

	// Set headers for file download
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))

	// Write content
	_, err = w.Write(content)
	if err != nil {
		s.logger.Error("Failed to write download content", "error", err, "id", id)
		return
	}

	s.logger.Info("Pipeline downloaded", "id", id, "name", pipeline.Name, "format", format)
}