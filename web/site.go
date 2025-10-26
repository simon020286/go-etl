package web

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
)

type message struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan message)

func StartServer(logger *slog.Logger) {
	// Create new API server
	apiServer, err := NewAPIServer(logger)
	if err != nil {
		logger.Error("Failed to create API server", "error", err)
		return
	}

	// Use the API server's router directly instead of the core web server
	router := apiServer.GetRouter()

	// Add legacy endpoints to the same router
	// router.HandleFunc("/legacy/ws", handleConnections)
	// router.HandleFunc("/legacy/start", handleStart(logger))
	// router.HandleFunc("/legacy/upload", handleUpload(logger))

	// Dashboard endpoint - serve the dashboard.html file directly
	router.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./web/static/dashboard.html")
	})

	// Static files under /static/ prefix
	router.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("./web/static/"))))

	go startWebSocket()

	// Restore pipelines that were running when server stopped
	logger.Info("Restoring running pipelines from previous session")
	if err := apiServer.RestoreRunningPipelines(); err != nil {
		logger.Error("Failed to restore running pipelines", "error", err)
	}

	fmt.Println("Starting server on :8080")
	fmt.Println("API endpoints available at: http://localhost:8080/api/v1/")
	fmt.Println("WebSocket endpoint: ws://localhost:8080/ws")
	fmt.Println("Health check: http://localhost:8080/api/v1/health")

	// Start server directly with our router
	if err := http.ListenAndServe(":8080", router); err != nil {
		logger.Error("Failed to start server", "error", err)
	}
}

func startWebSocket() {
	for {
		msg := <-broadcast
		jsonMsg, _ := json.Marshal(msg)
		for client := range clients {
			client.WriteMessage(websocket.TextMessage, []byte(jsonMsg))
		}
	}
}
