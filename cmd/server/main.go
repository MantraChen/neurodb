package main

import (
	"context"
	"log"
	"neurodb/pkg/api"
	"neurodb/pkg/core"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	dbFile := "neuro.db"

	store := core.NewHybridStore(dbFile)

	server := api.NewServer(store)

	go func() {
		server.Start(":8080")
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
