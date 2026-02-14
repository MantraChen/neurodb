package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"neurodb/pkg/api"
	"neurodb/pkg/config"
	"neurodb/pkg/core"
	"neurodb/pkg/network"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "", "Path to config file (default: configs/neuro.yaml or neuro.yaml)")
	flag.Parse()

	log.Println("[Main] Loading configuration...")
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Printf("[Warning] Failed to load config: %v. Using defaults.", err)
	}

	store := core.NewHybridStore(cfg)
	log.Printf("[Main] NeuroDB Kernel initialized (Shards: %d)", cfg.System.ShardCount)

	apiServer := api.NewServer(store)
	httpSrv := &http.Server{
		Addr:         cfg.Server.Addr,
		Handler:      nil,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		apiServer.RegisterRoutes()
		log.Printf("[HTTP] Listening on %s (Dashboard & API)...", cfg.Server.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[HTTP] Server failed: %v", err)
		}
	}()

	// TCP Server
	tcpServer := network.NewTCPServer(store)
	go func() {
		if err := tcpServer.Start(cfg.Server.TCPAddr); err != nil {
			log.Fatalf("[TCP] Server failed: %v", err)
		}
	}()

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("\n[Main] Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("[HTTP] Shutdown error: %v", err)
	}

	store.Close()
	log.Println("[Main] Storage closed. Bye.")
}
