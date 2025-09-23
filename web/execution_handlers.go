package web

import (
	"go-etl/db"
	"net/http"
	"strings"
)

// handleListExecutions handles GET /api/v1/executions
func (s *APIServer) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	offset := s.parseQueryInt(r, "offset", 0)
	limit := s.parseQueryInt(r, "limit", 20)
	pipelineID := s.parseQueryInt(r, "pipeline_id", 0)
	status := r.URL.Query().Get("status")
	orderBy := r.URL.Query().Get("order_by")
	orderDir := r.URL.Query().Get("order_dir")

	// Validate order direction
	if orderDir != "" && orderDir != "asc" && orderDir != "desc" {
		s.sendError(w, http.StatusBadRequest, "order_dir must be 'asc' or 'desc'")
		return
	}

	// Create request
	req := db.ListExecutionsRequest{
		PipelineID: pipelineID,
		Status:     status,
		Offset:     offset,
		Limit:      limit,
		OrderBy:    orderBy,
		OrderDir:   orderDir,
	}

	// Get executions
	executions, total, err := s.manager.Executions().ListExecutions(req)
	if err != nil {
		s.logger.Error("Failed to list executions", "error", err)
		s.sendError(w, http.StatusInternalServerError, "Failed to retrieve executions")
		return
	}

	s.sendPaginated(w, executions, total, offset, limit)
}

// handleGetExecution handles GET /api/v1/executions/{id}
func (s *APIServer) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	execution, err := s.manager.Executions().GetExecution(id)
	if err != nil {
		s.logger.Error("Failed to get execution", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Execution not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve execution")
		}
		return
	}

	s.sendSuccess(w, execution)
}

// handleGetExecutionLogs handles GET /api/v1/executions/{id}/logs
func (s *APIServer) handleGetExecutionLogs(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	limit := s.parseQueryInt(r, "limit", 100)
	if limit > 1000 {
		limit = 1000
	}

	logs, err := s.manager.Executions().GetExecutionLogs(id, limit)
	if err != nil {
		s.logger.Error("Failed to get execution logs", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to retrieve execution logs")
		return
	}

	s.sendSuccess(w, logs)
}

// handleDeleteExecution handles DELETE /api/v1/executions/{id}
func (s *APIServer) handleDeleteExecution(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Delete execution
	err = s.manager.Executions().DeleteExecution(id)
	if err != nil {
		s.logger.Error("Failed to delete execution", "error", err, "id", id)
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Execution not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to delete execution")
		}
		return
	}

	s.logger.Info("Execution deleted", "id", id)

	// Broadcast event
	s.BroadcastMessage("execution_deleted", map[string]interface{}{
		"execution_id": id,
	})

	s.sendSuccess(w, nil, "Execution deleted successfully")
}

// handleGetPipelineStats handles GET /api/v1/pipelines/{id}/stats
func (s *APIServer) handleGetPipelineStats(w http.ResponseWriter, r *http.Request) {
	id, err := s.extractID(r)
	if err != nil {
		s.sendError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Verify pipeline exists
	_, err = s.manager.Pipelines().GetPipeline(id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.sendError(w, http.StatusNotFound, "Pipeline not found")
		} else {
			s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline")
		}
		return
	}

	// Get stats
	stats, err := s.manager.Executions().GetExecutionStats(id)
	if err != nil {
		s.logger.Error("Failed to get pipeline stats", "error", err, "id", id)
		s.sendError(w, http.StatusInternalServerError, "Failed to retrieve pipeline statistics")
		return
	}

	s.sendSuccess(w, stats)
}

// handleListTemplates handles GET /api/v1/templates (placeholder)
func (s *APIServer) handleListTemplates(w http.ResponseWriter, r *http.Request) {
	// This will be implemented in a later step
	s.sendSuccess(w, []interface{}{}, "Templates feature not yet implemented")
}