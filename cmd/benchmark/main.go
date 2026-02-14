package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"neurodb/pkg/protocol"
	"time"
)

const (
	TotalRequests = 5000 // 测试请求总数
	TcpAddr       = "localhost:9090"
	HttpAddr      = "http://localhost:8080"
)

func main() {
	fmt.Printf("NeuroDB Protocol Benchmark (N=%d)\n", TotalRequests)
	fmt.Println("---------------------------------------------------")

	// 1. 测试 HTTP
	fmt.Println(">> Starting HTTP Benchmark (JSON over HTTP 1.1)...")
	httpDuration := runHTTPBenchmark()
	fmt.Printf("   HTTP Time: %v | QPS: %.0f\n\n", httpDuration, float64(TotalRequests)/httpDuration.Seconds())

	// 2. 测试 TCP
	fmt.Println(">> Starting TCP Benchmark (Binary Protocol)...")
	tcpDuration := runTCPBenchmark()
	fmt.Printf("   TCP  Time: %v | QPS: %.0f\n", tcpDuration, float64(TotalRequests)/tcpDuration.Seconds())

	// 3. 结果对比
	fmt.Println("---------------------------------------------------")
	speedup := httpDuration.Seconds() / tcpDuration.Seconds()
	fmt.Printf("Conclusion: TCP is %.2fx faster than HTTP!\n", speedup)
}

func runHTTPBenchmark() time.Duration {
	start := time.Now()
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}

	for i := 0; i < TotalRequests; i++ {
		data := map[string]interface{}{
			"key":   i,
			"value": "bench_data",
		}
		jsonData, _ := json.Marshal(data)

		resp, err := client.Post(HttpAddr+"/api/put", "application/json", bytes.NewReader(jsonData))
		if err != nil {
			log.Fatalf("HTTP Req failed: %v", err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	return time.Since(start)
}

func runTCPBenchmark() time.Duration {
	start := time.Now()

	conn, err := net.Dial("tcp", TcpAddr)
	if err != nil {
		log.Fatalf("TCP Connect failed: %v", err)
	}
	defer conn.Close()

	keyBuf := make([]byte, 8)
	val := []byte("bench_data")

	for i := 0; i < TotalRequests; i++ {
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
