package steps

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"go-etl/core"
	"go-etl/pipeline"
	"go-etl/web"
)

type webhookResponse struct {
	Value map[string]any
}

type WebhookStep struct {
	name    string
	trigger chan webhookResponse
	method  string
	path    string
}

func (s *WebhookStep) Name() string { return s.name }

func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// Wait for the trigger to be sent
	<-s.trigger
	// Here you would implement the logic to handle the webhook request
	// For example, you could send a response back or process the request
	return core.CreateDefaultResultData("Webhook triggered"), nil
}

func (s *WebhookStep) SetOnTrigger(callback func(data map[string]*core.Data)) error {
	// Register webhook endpoint when the pipeline actually runs
	registry := web.GetWebhookRegistry()
	router := registry.GetRouter()
	if router != nil {
		router.HandleFunc("/webhook/"+s.path, func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Received webhook request", slog.String("name", s.name), slog.String("method", r.Method))

			data := make(map[string]any)
			switch r.Method {
			case "POST":
				contentType := r.Header.Get("Content-Type") // You can process the body if needed
				slog.Info("Content-Type", slog.String("type", contentType))
				switch contentType {
				case "application/json":
					// Handle JSON body if needed
					decoder := json.NewDecoder(r.Body)
					if err := decoder.Decode(&data); err != nil {
						http.Error(w, "Invalid JSON", http.StatusBadRequest)
						return
					}
				case "application/x-www-form-urlencoded":
					// Handle form data if needed
					if err := r.ParseForm(); err != nil {
						http.Error(w, "Invalid form data", http.StatusBadRequest)
						return
					}
					for key, values := range r.Form {
						if len(values) > 0 {
							data[key] = values[0] // Take the first value for simplicity
						}
					}
				case "text/plain":
					// Handle plain text body if needed
					bodyBytes, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, "Failed to read body", http.StatusInternalServerError)
						return
					}
					data["body"] = string(bodyBytes)
				default:
					slog.Warn("Unsupported Content-Type", slog.String("type", contentType))
				}
			case "GET":
				// Handle query parameters for GET requests
				for key, values := range r.URL.Query() {
					if len(values) > 0 {
						data[key] = values[0] // Take the first value for simplicity
					}
				}
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return

			}

			s.trigger <- webhookResponse{Value: data}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Webhook triggered"))
		}).Methods(strings.ToUpper(s.method))

		slog.Info("Webhook endpoint registered", slog.String("path", "/webhook/"+s.path), slog.String("method", strings.ToUpper(s.method)))
	} else {
		slog.Warn("APIServer router not available, webhook endpoint not registered", "path", s.path)
	}

	// Start goroutine to handle incoming webhook events
	go func() {
		for t := range s.trigger {
			slog.Info("Webhook triggered", slog.Attr{Key: "value", Value: slog.AnyValue(t.Value)})
			callback(core.CreateDefaultResultData(t.Value))
		}
	}()

	return nil
}

func init() {
	pipeline.RegisterTriggerType("webhook", func(name string, config map[string]any) (core.Step, error) {
		method, ok := config["method"].(string)
		if !ok {
			method = "GET" // Default to GET if not specified
		}

		path, ok := config["path"].(string)
		if !ok {
			path = name // Default path if not specified
		}

		trigger := make(chan webhookResponse)

		return &WebhookStep{
			name:    name,
			trigger: trigger,
			method:  method,
			path:    path,
		}, nil
	})
}
