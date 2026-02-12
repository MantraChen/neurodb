package main

import (
	"neurodb/pkg/api"
	"neurodb/pkg/core"
)

func main() {
	dbFile := "neuro.db"

	store := core.NewHybridStore(dbFile)
	defer store.Close()

	server := api.NewServer(store)

	server.Start(":8080")
}
