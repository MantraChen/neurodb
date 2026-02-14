package main

import (
	"context"
	"log"
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
	log.Println("[Main] Loading configuration...")
	cfg, err := config.Load("configs/neuro.yaml")
	if err != nil {
		log.Printf("[Warning] Failed to load config: %v. Using defaults.", err)
	}

	store := core.NewHybridStore(cfg)
	log.Printf("[Main] NeuroDB Kernel initialized (Shards: %d)", cfg.System.ShardCount)

	httpServer := api.NewServer(store)
	go func() {
		httpServer.Start(cfg.Server.Addr) // :8080
	}()

	tcpServer := network.NewTCPServer(store)
	go func() {
		if err := tcpServer.Start(":9090"); err != nil {
			log.Fatalf("TCP Server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("\n[Main] Shutting down...")

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store.Close()
	log.Println("[Main] Storage closed. Bye.")
}
