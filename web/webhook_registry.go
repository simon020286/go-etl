package web

import (
	"sync"

	"github.com/gorilla/mux"
)

// WebhookRegistry provides access to the main router for webhook registration
type WebhookRegistry struct {
	router *mux.Router
	mu     sync.RWMutex
}

var (
	registry     *WebhookRegistry
	registryOnce sync.Once
)

// GetWebhookRegistry returns the singleton webhook registry
func GetWebhookRegistry() *WebhookRegistry {
	registryOnce.Do(func() {
		registry = &WebhookRegistry{}
	})
	return registry
}

// SetRouter sets the main router for webhook registration
func (wr *WebhookRegistry) SetRouter(router *mux.Router) {
	wr.mu.Lock()
	defer wr.mu.Unlock()
	wr.router = router
}

// GetRouter returns the router for webhook registration
func (wr *WebhookRegistry) GetRouter() *mux.Router {
	wr.mu.RLock()
	defer wr.mu.RUnlock()
	return wr.router
}