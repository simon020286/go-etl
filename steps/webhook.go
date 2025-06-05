package steps

import (
	"context"
	"go-etl/core"
	"net/http"
)

type WebhookStep struct {
	name    string
	trigger chan struct{}
}

func (s *WebhookStep) Name() string { return s.name }

func (s *WebhookStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	// Wait for the trigger to be sent
	<-s.trigger
	// Here you would implement the logic to handle the webhook request
	// For example, you could send a response back or process the request
	return core.CreateDefaultResultData("Webhook triggered"), nil
}

func (s *WebhookStep) Trigger() error {
	// This method is not used for webhook steps, as they are triggered by HTTP requests
	return nil
}

func newWebhookStep(name string, config map[string]any) (core.Step, error) {
	trigger := make(chan struct{})
	core.GetWebServer().Mux().HandleFunc("/webhook/"+name, func(w http.ResponseWriter, r *http.Request) {
		// Trigger the webhook step when a request is received
		select {
		case trigger <- struct{}{}:
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Webhook triggered"))
		default:
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Webhook already triggered"))
		}
	})
	return &WebhookStep{name: name, trigger: trigger}, nil
}
