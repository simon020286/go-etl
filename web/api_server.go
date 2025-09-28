package web

import (
	"encoding/json"
	"fmt"
	"go-etl/db"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// APIServer handles REST API endpoints for pipeline management
type APIServer struct {
	manager   *db.Manager
	logger    *slog.Logger
	router    *mux.Router
	clients   map[*websocket.Conn]bool
	broadcast chan APIMessage
	upgrader  websocket.Upgrader
}

// APIMessage represents a WebSocket message
type APIMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// APIResponse represents a standard API response
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

// PaginatedResponse represents a paginated API response
type PaginatedResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
	Total   int         `json:"total"`
	Offset  int         `json:"offset"`
	Limit   int         `json:"limit"`
	Error   string      `json:"error,omitempty"`
}

// NewAPIServer creates a new API server instance
func NewAPIServer(logger *slog.Logger) (*APIServer, error) {
	manager, err := db.GetManager()
	if err != nil {
		return nil, fmt.Errorf("failed to get database manager: %w", err)
	}

	server := &APIServer{
		manager:   manager,
		logger:    logger,
		router:    mux.NewRouter(),
		clients:   make(map[*websocket.Conn]bool),
		broadcast: make(chan APIMessage, 100),
		upgrader:  websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}

	server.setupRoutes()
	go server.handleWebSocketBroadcast()

	return server, nil
}

// setupRoutes configures all API routes
func (s *APIServer) setupRoutes() {
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Pipeline CRUD routes - specific routes FIRST, then parameterized routes
	api.HandleFunc("/pipelines", s.handleListPipelines).Methods("GET")
	api.HandleFunc("/pipelines", s.handleCreatePipeline).Methods("POST")
	api.HandleFunc("/pipelines/upload", s.handleUploadPipeline).Methods("POST")
	api.HandleFunc("/pipelines/running", s.handleGetRunningPipelines).Methods("GET") // MOVED UP
	api.HandleFunc("/pipelines/{id}", s.handleGetPipeline).Methods("GET")
	api.HandleFunc("/pipelines/{id}", s.handleUpdatePipeline).Methods("PUT")
	api.HandleFunc("/pipelines/{id}", s.handleDeletePipeline).Methods("DELETE")
	api.HandleFunc("/pipelines/{id}/download", s.handleDownloadPipeline).Methods("GET")

	// Pipeline control routes
	api.HandleFunc("/pipelines/{id}/start", s.handleStartPipeline).Methods("POST")
	api.HandleFunc("/pipelines/{id}/stop", s.handleStopPipeline).Methods("POST")
	api.HandleFunc("/pipelines/{id}/pause", s.handlePausePipeline).Methods("POST")
	api.HandleFunc("/pipelines/{id}/resume", s.handleResumePipeline).Methods("POST")
	api.HandleFunc("/pipelines/{id}/status", s.handleGetPipelineStatus).Methods("GET")

	// Execution routes
	api.HandleFunc("/executions", s.handleListExecutions).Methods("GET")
	api.HandleFunc("/executions/{id}", s.handleGetExecution).Methods("GET")
	api.HandleFunc("/executions/{id}/logs", s.handleGetExecutionLogs).Methods("GET")
	api.HandleFunc("/executions/{id}", s.handleDeleteExecution).Methods("DELETE")

	// Statistics routes
	api.HandleFunc("/pipelines/{id}/stats", s.handleGetPipelineStats).Methods("GET")

	// Templates routes (will be implemented later)
	api.HandleFunc("/templates", s.handleListTemplates).Methods("GET")

	// WebSocket endpoint
	s.router.HandleFunc("/ws", s.handleWebSocket)

	// Health check
	api.HandleFunc("/health", s.handleHealth).Methods("GET")

	// CORS middleware
	s.router.Use(s.corsMiddleware)
}

// GetRouter returns the configured router
func (s *APIServer) GetRouter() *mux.Router {
	return s.router
}

// corsMiddleware adds CORS headers
func (s *APIServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions for responses
func (s *APIServer) sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func (s *APIServer) sendError(w http.ResponseWriter, statusCode int, message string) {
	s.sendJSON(w, statusCode, APIResponse{
		Success: false,
		Error:   message,
	})
}

func (s *APIServer) sendSuccess(w http.ResponseWriter, data interface{}, message ...string) {
	response := APIResponse{
		Success: true,
		Data:    data,
	}
	if len(message) > 0 {
		response.Message = message[0]
	}
	s.sendJSON(w, http.StatusOK, response)
}

func (s *APIServer) sendPaginated(w http.ResponseWriter, data interface{}, total, offset, limit int) {
	s.sendJSON(w, http.StatusOK, PaginatedResponse{
		Success: true,
		Data:    data,
		Total:   total,
		Offset:  offset,
		Limit:   limit,
	})
}

// Helper function to extract ID from URL
func (s *APIServer) extractID(r *http.Request) (int, error) {
	vars := mux.Vars(r)
	idStr, ok := vars["id"]
	if !ok {
		return 0, fmt.Errorf("missing id parameter")
	}

	id, err := strconv.Atoi(idStr)
	if err != nil {
		return 0, fmt.Errorf("invalid id parameter: %s", idStr)
	}

	return id, nil
}

// Helper function to parse query parameters
func (s *APIServer) parseQueryInt(r *http.Request, key string, defaultValue int) int {
	value := r.URL.Query().Get(key)
	if value == "" {
		return defaultValue
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return parsed
}

func (s *APIServer) parseQueryBool(r *http.Request, key string) *bool {
	value := r.URL.Query().Get(key)
	if value == "" {
		return nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return nil
	}

	return &parsed
}

// Health check endpoint
func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.sendSuccess(w, map[string]string{
		"status":  "ok",
		"service": "go-etl-api",
	})
}

// WebSocket handling
func (s *APIServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed", "error", err)
		return
	}
	defer ws.Close()

	s.clients[ws] = true
	s.logger.Info("WebSocket client connected")

	// Send welcome message
	welcome := APIMessage{
		Type: "welcome",
		Data: map[string]string{"message": "Connected to go-etl API"},
	}
	ws.WriteJSON(welcome)

	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			delete(s.clients, ws)
			s.logger.Info("WebSocket client disconnected")
			break
		}
	}
}

func (s *APIServer) handleWebSocketBroadcast() {
	for {
		msg := <-s.broadcast
		for client := range s.clients {
			err := client.WriteJSON(msg)
			if err != nil {
				client.Close()
				delete(s.clients, client)
			}
		}
	}
}

// BroadcastMessage sends a message to all connected WebSocket clients
func (s *APIServer) BroadcastMessage(messageType string, data interface{}) {
	select {
	case s.broadcast <- APIMessage{Type: messageType, Data: data}:
	default:
		// Channel is full, skip message
	}
}