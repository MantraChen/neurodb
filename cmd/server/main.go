package main

import (
	"context"
	"log"
	"neurodb/pkg/api"
	"neurodb/pkg/config"
	"neurodb/pkg/core"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log.Println("[Main] Loading configuration...")
	cfg, err := config.Load("configs/neuro.yaml")
	if err != nil {
		log.Printf("[Warning] Failed to load config: %v. Using defaults.", err)
	}

	store := core.NewHybridStore(cfg)
	log.Printf("[Main] NeuroDB Kernel initialized (Shards: %d, WAL Buffer: %d)",
		cfg.System.ShardCount, cfg.Storage.WalBufferSize)

	server := api.NewServer(store)
	go func() {
		server.Start(cfg.Server.Addr)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("\n[Main] Shutting down server...")

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store.Close()
	log.Println("[Main] Storage engine closed.")
	log.Println("[Main] Server exited cleanly.")
}
