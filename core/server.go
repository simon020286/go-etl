package core

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"sync"
)

type WebServer struct {
	mux    *http.ServeMux
	server *http.Server
	status http.ConnState
}

var (
	lock     = &sync.Mutex{}
	instance *WebServer
)

func NewWebServer(addr string) *WebServer {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return &WebServer{
		mux:    mux,
		server: server,
		status: http.StateClosed,
	}
}

func GetWebServer() *WebServer {
	lock.Lock()
	defer lock.Unlock()
	if instance == nil {
		instance = NewWebServer(":8080") // Default address, can be changed later
	}
	return instance
}

func StartWebServer() {
	if instance == nil {
		return
	}

	instance.Start()
}

func StopWebServer(ctx context.Context) error {
	if instance == nil {
		return nil
	}
	return instance.Stop(ctx)
}

func (ws *WebServer) Mux() *http.ServeMux {
	return ws.mux
}

func (ws *WebServer) Start() {
	if ws.status == http.StateActive {
		slog.Info("Server is already running")
		return

	}
	go func() {
		ws.status = http.StateActive

		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v", err)
		}
		slog.Info("Server stopped")
	}()
}

func (ws *WebServer) Stop(ctx context.Context) error {
	if err := ws.server.Shutdown(ctx); err != nil {
		slog.Error("Error stopping server:", slog.String("error", err.Error()))
		return err
	}
	return nil
}
