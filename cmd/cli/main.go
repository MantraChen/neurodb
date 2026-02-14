package main

import (
	"bufio"
	"fmt"
	"neurodb/pkg/client"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	ServerAddr = "localhost:9090"
	Prompt     = "neuro> "
)

func main() {
	fmt.Println("NeuroDB CLI v1.0")
	fmt.Println("Connecting to " + ServerAddr + "...")

	cli, err := client.Dial(ServerAddr)
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
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

func printHelp() {
	fmt.Println(`
Commands:
  put <key> <value>   Insert a record (Key must be integer)
  get <key>           Retrieve a record
  exit                Exit the CLI
	`)
}
