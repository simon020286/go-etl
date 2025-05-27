package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-etl/core"
	"go-etl/pipeline"
	_ "go-etl/steps"
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

var clients = make(map[*websocket.Conn]bool)
var broadcast = make(chan string)

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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // o slog.LevelInfo
	}))

	go handleMessages()
	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/upload", handleUpload(logger))
	http.HandleFunc("/print", handlePrint(logger))
	http.Handle("/", http.FileServer(http.Dir("./web")))

	log.Println("Server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))

	// f, err := os.Open("pipeline.yaml")
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()

	// var config pipeline.PipelineConfig
	// dec := yaml.NewDecoder(f)
	// if err := dec.Decode(&config); err != nil {
	// 	panic(err)
	// }

	// pl, err := pipeline.LoadPipeline(config)
	// if err != nil {
	// 	panic(err)
	// }

	// _, err = pl.Run(context.Background(), logger)
	// if err != nil {
	// 	panic(err)
	// }

}

func handleMessages() {
	for {
		msg := <-broadcast
		for client := range clients {
			client.WriteMessage(websocket.TextMessage, []byte(msg))
		}
	}
}

func logToClients(msg string) {
	broadcast <- msg
}

func handlePrint(logger *slog.Logger) http.HandlerFunc {
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
func handleUpload(logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		file, _, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "File error", 400)
			return
		}
		defer file.Close()

		// out, err := os.Create("pipeline.yaml")
		// if err != nil {
		// 	http.Error(w, "Save error", 500)
		// 	return
		// }
		// defer out.Close()

		// io.Copy(out, file)
		logToClients("Pipeline uploaded.")
		// fmt.Fprintln(w, "Uploaded")
		logToClients("Pipeline starting...")
		dec := yaml.NewDecoder(file)
		go func() {
			var config pipeline.PipelineConfig
			if err := dec.Decode(&config); err != nil {
				panic(err)
			}

			pl, err := pipeline.LoadPipeline(config)
			if err != nil {
				panic(err)
			}

			pl.OnChange = func(step string, data *core.Data) {
				jsonData, _ := json.Marshal(data.Value)
				logToClients(fmt.Sprintf("Step %s completed with data: %s", step, jsonData))
			}

			if err := pl.Run(context.Background(), logger); err != nil {
				panic(err)
			}

		}()
		// fmt.Fprintln(w, "Started")
	}
}
