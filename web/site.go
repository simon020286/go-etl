package web

import (
	"context"
	"encoding/json"
	"fmt"
	"go-etl/core"
	"go-etl/pipeline"
	"log/slog"
	"net/http"

	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

type message struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan message)

var upgrader = websocket.Upgrader{}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer ws.Close()
	clients[ws] = true

	for {
		_, _, err := ws.ReadMessage()
		if err != nil {
			delete(clients, ws)
			break
		}
	}
}

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
	router.HandleFunc("/legacy/ws", handleConnections)
	router.HandleFunc("/legacy/start", handleStart(logger))
	router.HandleFunc("/legacy/upload", handleUpload(logger))

	// Static files
	router.PathPrefix("/").Handler(http.FileServer(http.Dir("./web/static/")))

	go startWebSocket()

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

func logToClients(path string, msg string) {
	message := message{Path: path, Message: msg}
	broadcast <- message
}

func handleUpload(_ *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "File error", 400)
			return
		}
		defer file.Close()
		dec := yaml.NewDecoder(file)
		var config pipeline.PipelineConfig
		if err := dec.Decode(&config); err != nil {
			panic(err)
		}

		w.Header().Add("Content-Type", "application/json")
		jsonConfig, err := json.Marshal(config)
		if err != nil {
			http.Error(w, "JSON error", 500)
			return
		}

		fmt.Fprintln(w, string(jsonConfig))
	}
}

func handleStart(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "File error", 400)
			return
		}
		defer file.Close()
		dec := yaml.NewDecoder(file)
		var config pipeline.PipelineConfig
		if err := dec.Decode(&config); err != nil {
			panic(err)
		}

		w.Header().Add("Content-Type", "application/json")
		jsonConfig, err := json.Marshal(config)
		if err != nil {
			http.Error(w, "JSON error", 500)
			return
		}
		logToClients("status", "Pipeline starting...")
		go func() {
			pl, err := pipeline.LoadPipeline(config)
			if err != nil {
				panic(err)
			}

			pl.OnChange = func(event core.ChangeEvent) {
				var data string
				if event.Data != nil {
					jsonData, _ := json.Marshal(event.Data)
					data = string(jsonData)
				}
				logToClients("step/"+string(event.Type)+"/"+event.StepName, data)
			}

			if err := pl.Run(context.Background(), logger); err != nil {
				panic(err)
			}

		}()
		fmt.Fprintln(w, string(jsonConfig))
	}
}
