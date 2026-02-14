package main

import (
	"fmt"
	"log"
	"neurodb/pkg/client"
	"time"
)

func main() {
	fmt.Println("Connecting to NeuroDB...")
	cli, err := client.Dial("localhost:9090")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer cli.Close()

	key := int64(10086)
	value := "Hello, NeuroDB SDK!"

	fmt.Printf("Writing: Key=%d, Val=%s\n", key, value)
	start := time.Now()
	if err := cli.Put(key, []byte(value)); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("Put done in %v\n", time.Since(start))

	fmt.Printf("Reading Key=%d...\n", key)
	start = time.Now()
	val, err := cli.Get(key)
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Got Value: %s (in %v)\n", string(val), time.Since(start))
}
