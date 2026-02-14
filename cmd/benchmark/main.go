package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"neurodb/pkg/protocol"
	"time"
)

func main() {
	httpAddr := flag.String("http", "http://localhost:8080", "HTTP API base URL")
	tcpAddr := flag.String("tcp", "localhost:9090", "TCP server address")
	nReq := flag.Int("n", 5000, "Number of requests per run")
	flag.Parse()

	fmt.Printf("NeuroDB Protocol Benchmark (N=%d)\n", *nReq)
	fmt.Printf("  HTTP=%s  TCP=%s\n", *httpAddr, *tcpAddr)
	fmt.Println("---------------------------------------------------")

	fmt.Println(">> Starting HTTP Benchmark (JSON over HTTP 1.1)...")
	httpDuration := runHTTPBenchmark(*httpAddr, *nReq)
	fmt.Printf("   HTTP Time: %v | QPS: %.0f\n\n", httpDuration, float64(*nReq)/httpDuration.Seconds())

	fmt.Println(">> Starting TCP Benchmark (Binary Protocol)...")
	tcpDuration := runTCPBenchmark(*tcpAddr, *nReq)
	fmt.Printf("   TCP  Time: %v | QPS: %.0f\n", tcpDuration, float64(*nReq)/tcpDuration.Seconds())

	fmt.Println("---------------------------------------------------")
	speedup := httpDuration.Seconds() / tcpDuration.Seconds()
	fmt.Printf("Conclusion: TCP is %.2fx faster than HTTP!\n", speedup)
}

func runHTTPBenchmark(httpAddr string, n int) time.Duration {
	start := time.Now()
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}

	for i := 0; i < n; i++ {
		data := map[string]interface{}{
			"key":   i,
			"value": "bench_data",
		}
		jsonData, _ := json.Marshal(data)

		resp, err := client.Post(httpAddr+"/api/put", "application/json", bytes.NewReader(jsonData))
		if err != nil {
			log.Fatalf("HTTP Req failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return time.Since(start)
}

func runTCPBenchmark(addr string, n int) time.Duration {
	start := time.Now()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("TCP Connect failed: %v", err)
	}
	defer conn.Close()

	keyBuf := make([]byte, 8)
	val := []byte("bench_data")

	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(keyBuf, uint64(i))

		err := protocol.Encode(conn, protocol.OpPut, keyBuf, val)
		if err != nil {
			log.Fatalf("TCP Write failed: %v", err)
		}

		_, err = protocol.Decode(conn)
		if err != nil {
			log.Fatalf("TCP Read failed: %v", err)
		}
	}

	return time.Since(start)
}
