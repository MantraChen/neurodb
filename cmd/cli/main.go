package main

import (
	"bufio"
	"flag"
	"fmt"
	"neurodb/pkg/client"
	"os"
	"strconv"
	"strings"
	"time"
)

const Prompt = "neuro> "

func main() {
	serverAddr := flag.String("addr", "localhost:9090", "NeuroDB TCP Server Address")
	flag.Parse()

	fmt.Printf("NeuroDB CLI v2.0 (Target: %s)\n", *serverAddr)
	fmt.Println("Connecting...")

	cli, err := client.Dial(*serverAddr)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		fmt.Println("Tip: Ensure the server is running (e.g. go run cmd/server/main.go).")
		return
	}
	defer cli.Close()
	fmt.Println("Connected! Type 'help' for commands.")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(Prompt)
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "put", "set":
			handlePut(cli, parts)
		case "get":
			handleGet(cli, parts)
		case "del", "rm":
			handleDel(cli, parts)
		case "scan":
			handleScan(cli, parts)
		case "help":
			printHelp()
		case "exit", "quit":
			fmt.Println("Bye!")
			return
		default:
			fmt.Printf("Unknown command: '%s'. Type 'help'.\n", cmd)
		}
	}
}

func handlePut(cli *client.Client, parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: put <key_int> <value_string>")
		return
	}

	key, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		fmt.Println("Error: Key must be an integer (e.g., 1001)")
		return
	}

	value := strings.Join(parts[2:], " ")

	start := time.Now()
	err = cli.Put(key, []byte(value))
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("OK (%v)\n", duration)
	}
}

func handleGet(cli *client.Client, parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: get <key_int>")
		return
	}

	key, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		fmt.Println("Error: Key must be an integer")
		return
	}

	start := time.Now()
	val, err := cli.Get(key)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("\"%s\" (%v)\n", string(val), duration)
	}
}

func handleDel(cli *client.Client, parts []string) {
	if len(parts) < 2 {
		fmt.Println("Usage: del <key_int>")
		return
	}

	key, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		fmt.Println("Error: Key must be an integer")
		return
	}

	start := time.Now()
	err = cli.Delete(key)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Deleted (%v)\n", duration)
	}
}

func handleScan(cli *client.Client, parts []string) {
	if len(parts) < 3 {
		fmt.Println("Usage: scan <start_key> <end_key>")
		return
	}

	startKey, err1 := strconv.ParseInt(parts[1], 10, 64)
	endKey, err2 := strconv.ParseInt(parts[2], 10, 64)

	if err1 != nil || err2 != nil {
		fmt.Println("Error: Keys must be integers")
		return
	}

	fmt.Printf("Scanning range [%d, %d]...\n", startKey, endKey)
	start := time.Now()
	records, err := cli.Scan(startKey, endKey)
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Found %d records (%v):\n", len(records), duration)
	count := 0
	for _, rec := range records {
		if count >= 20 {
			fmt.Printf("... and %d more\n", len(records)-20)
			break
		}
		fmt.Printf("  [%d] -> %s\n", rec.Key, string(rec.Value))
		count++
	}
}

func printHelp() {
	fmt.Println(`
Commands:
  put <key> <value>      Insert/Update record
  get <key>              Retrieve record
  del <key>              Delete record
  scan <start> <end>     Range query (inclusive)
  exit                   Exit CLI
	`)
}
