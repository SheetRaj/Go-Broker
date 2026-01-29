package main

import (
	"fmt"
	"log"
	"net"

	"github.com/SheetRaj/go-broker/internal/config"
	"github.com/SheetRaj/go-broker/internal/network"
	"github.com/SheetRaj/go-broker/internal/storage"
)

func main() {

	// 1. Load Config
	cfg, _ := config.Load("config.json")

	// 2. Initialize WAL (Single topic 'orders' for now)
	wal, err := storage.NewWAL(cfg.Storage.DataDir, "orders")
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// 3. Start TCP Listener
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Bind failed: %v", err)
	}

	fmt.Println("Server Listening on :9000")
	fmt.Println("Try: echo 'PUB orders 5\\nhello' | nc localhost 9000")

	// 4. Accept Connections Loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connection error: %v", err)
			continue
		}

		// Handle each client in a separate Goroutine (Concurrency!)
		go network.HandleConnection(conn, wal)
	}
}
