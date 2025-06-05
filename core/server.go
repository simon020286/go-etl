package core

import (
	"context"
	"log"
	"net/http"
	"sync"
)

type WebServer struct {
	mux    *http.ServeMux
	server *http.Server
}

var lock = &sync.Mutex{}
var instance *WebServer

func NewWebServer(addr string) *WebServer {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return &WebServer{
		mux:    mux,
		server: server,
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
	if ws.server.ConnState != nil {
		log.Println("Server is already running")
		return

	}
	go func() {
		if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not start server: %v", err)
		}
	}()
}

func (ws *WebServer) Stop(ctx context.Context) error {
	if err := ws.server.Shutdown(ctx); err != nil {
		log.Printf("Error stopping server: %v", err)
		return err
	}
	return nil
}
