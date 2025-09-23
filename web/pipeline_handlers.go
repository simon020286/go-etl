package web

import (
	"encoding/json"
	"go-etl/db"
	"net/http"
	"strings"
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

	status, err := s.manager.PipelineState().GetPipelineStatus(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline status", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline status")
		}
		return
	}

	isRunning := s.manager.PipelineState().IsRunning(id)

	s.sendSuccess(w, map[string]interface{}{
		"pipeline_id": id,
		"status":      status,
		"is_running":  isRunning,
	})
}

// Pipeline control handlers (placeholder implementations for now)
func (s *APIServer) handleStartPipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// This will be implemented in the next step
	s.sendError(w, http.StatusNotImplemented, "Pipeline execution control not yet implemented")
	s.logger.Info("Start pipeline requested", "id", id)
}

func (s *APIServer) handleStopPipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// This will be implemented in the next step
	s.sendError(w, http.StatusNotImplemented, "Pipeline execution control not yet implemented")
	s.logger.Info("Stop pipeline requested", "id", id)
}

func (s *APIServer) handlePausePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// This will be implemented in the next step
	s.sendError(w, http.StatusNotImplemented, "Pipeline execution control not yet implemented")
	s.logger.Info("Pause pipeline requested", "id", id)
}

func (s *APIServer) handleResumePipeline(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// This will be implemented in the next step
	s.sendError(w, http.StatusNotImplemented, "Pipeline execution control not yet implemented")
	s.logger.Info("Resume pipeline requested", "id", id)
}