package steps

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"go-etl/core"
)

type webhookResponse struct {
	Value map[string]any
}

type WebhookStep struct {
	name    string
	trigger chan webhookResponse
	method  string
}

func (s *WebhookStep) Name() string { return s.name }

func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// Wait for the trigger to be sent
	<-s.trigger
	// Here you would implement the logic to handle the webhook request
	// For example, you could send a response back or process the request
	return core.CreateDefaultResultData("Webhook triggered"), nil
}

func (s *WebhookStep) SetOnTrigger(callback func()) error {
	// This method is not used for webhook steps, as they are triggered by HTTP requests
	go func() {
		for t := range s.trigger {
			slog.Info("Webhook triggered", slog.Attr{Key: "value", Value: slog.AnyValue(t.Value)})
			callback()
		}
	}()
	return nil
}

func newWebhookStep(name string, config map[string]any) (core.Step, error) {
	method, ok := config["method"].(string)
	if !ok {
		method = "GET" // Default to GET if not specified
	}

	trigger := make(chan webhookResponse)
	core.GetWebServer().Mux().HandleFunc("/webhook/"+name, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		slog.Info("Received webhook request", slog.String("name", name), slog.String("method", r.Method))

		data := make(map[string]any)
		if r.Method == "POST" {
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
		} else if r.Method == "GET" {
			// Handle query parameters for GET requests
			for key, values := range r.URL.Query() {
				if len(values) > 0 {
					data[key] = values[0] // Take the first value for simplicity
				}
			}
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return

		}

		trigger <- webhookResponse{Value: data}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Webhook triggered"))
	})

	return &WebhookStep{name: name, trigger: trigger, method: method}, nil
}
