package steps

import (
	"context"
	"encoding/json"
	"fmt"
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
	name         string
	trigger      chan webhookResponse
	method       string
	pathTemplate *core.InterpolateValue[string]
	resolvedPath string
	stopChan     chan struct{}
	registry     *web.WebhookRegistry
}

func (s *WebhookStep) Name() string { return s.name }

func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// Resolve path template with execution ID
	pathValue, err := s.pathTemplate.Resolve(state)
	if err != nil {
		return nil, err
	}
	s.resolvedPath = pathValue

	// Register webhook handler (now with dynamic path)
	router := s.registry.GetRouter()
	if router != nil {
		fullPath := "/webhook/" + s.resolvedPath
		router.HandleFunc(fullPath, func(w http.ResponseWriter, r *http.Request) {
			slog.Info("Received webhook request", slog.String("name", s.name), slog.String("method", r.Method))

			data := make(map[string]any)
			switch r.Method {
			case "POST":
				contentType := r.Header.Get("Content-Type")
				slog.Info("Content-Type", slog.String("type", contentType))
				switch contentType {
				case "application/json":
					decoder := json.NewDecoder(r.Body)
					if err := decoder.Decode(&data); err != nil {
						http.Error(w, "Invalid JSON", http.StatusBadRequest)
						return
					}
				case "application/x-www-form-urlencoded":
					if err := r.ParseForm(); err != nil {
						http.Error(w, "Invalid form data", http.StatusBadRequest)
						return
					}
					for key, values := range r.Form {
						if len(values) > 0 {
							data[key] = values[0]
						}
					}
				case "text/plain":
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
				for key, values := range r.URL.Query() {
					if len(values) > 0 {
						data[key] = values[0]
					}
				}
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}

			// Send data to trigger channel
			select {
			case s.trigger <- webhookResponse{Value: data}:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Webhook triggered"))
			case <-s.stopChan:
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("Pipeline stopped"))
			}
		}).Methods(strings.ToUpper(s.method))

		slog.Info("Webhook endpoint registered", slog.String("path", fullPath), slog.String("method", strings.ToUpper(s.method)))
	} else {
		slog.Warn("APIServer router not available, webhook endpoint not registered", "path", s.resolvedPath)
	}

	// Wait for first trigger (one-shot mode)
	response := <-s.trigger

	// Unregister webhook after use (one-shot mode)
	// Note: gorilla/mux doesn't support unregistration, but we close stopChan
	// which will make the handler return 503
	if s.stopChan != nil {
		close(s.stopChan)
	}

	slog.Info("Webhook triggered, returning data", slog.String("name", s.name))
	return core.CreateDefaultResultData(response.Value), nil
}

func (s *WebhookStep) SetOnTrigger(callback func(data map[string]*core.Data)) error {
	s.stopChan = make(chan struct{})

	// Resolve path template with empty state (for continuous mode, path is static)
	emptyState := &core.PipelineState{
		Results: make(map[string]map[string]*core.Data),
		Logger:  slog.Default(),
	}
	pathValue, err := s.pathTemplate.Resolve(emptyState)
	if err != nil {
		return fmt.Errorf("failed to resolve webhook path: %w", err)
	}
	s.resolvedPath = pathValue

	// Register webhook endpoint when the pipeline actually runs
	router := s.registry.GetRouter()
	if router != nil {
		fullPath := "/webhook/" + s.resolvedPath
		router.HandleFunc(fullPath, func(w http.ResponseWriter, r *http.Request) {
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

			// Check if trigger is still active before sending
			select {
			case s.trigger <- webhookResponse{Value: data}:
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Webhook triggered"))
			case <-s.stopChan:
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("Pipeline stopped"))
			}
		}).Methods(strings.ToUpper(s.method))

		slog.Info("Webhook endpoint registered", slog.String("path", fullPath), slog.String("method", strings.ToUpper(s.method)))
	} else {
		slog.Warn("APIServer router not available, webhook endpoint not registered", "path", s.resolvedPath)
	}

	// Start goroutine to handle incoming webhook events
	go func() {
		for {
			select {
			case t := <-s.trigger:
				slog.Info("Webhook triggered", slog.Attr{Key: "value", Value: slog.AnyValue(t.Value)})
				callback(core.CreateDefaultResultData(t.Value))
			case <-s.stopChan:
				slog.Info("Webhook trigger stopped", slog.String("name", s.name))
				return
			}
		}
	}()

	return nil
}

func (s *WebhookStep) Stop() error {
	if s.stopChan != nil {
		close(s.stopChan)
		slog.Info("Stopping webhook trigger", slog.String("name", s.name), slog.String("path", "/webhook/"+s.resolvedPath))
	}
	// Note: HTTP handlers cannot be unregistered from gorilla/mux
	// The handler will return 503 after stop due to stopChan check
	return nil
}

func init() {
	pipeline.RegisterTriggerType("webhook", func(name string, config map[string]any) (core.Step, error) {
		method, ok := config["method"].(string)
		if !ok {
			method = "POST" // Default to POST if not specified
		}

		path, ok := config["path"].(string)
		if !ok {
			path = name // Default path if not specified
		}

		trigger := make(chan webhookResponse, 1)

		return &WebhookStep{
			name:         name,
			trigger:      trigger,
			method:       method,
			pathTemplate: &core.InterpolateValue[string]{Raw: path},
			stopChan:     make(chan struct{}),
			registry:     web.GetWebhookRegistry(),
		}, nil
	})
}
