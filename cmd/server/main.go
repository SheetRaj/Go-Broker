package main

import (
	"fmt"
	"log"
	"net"

	"github.com/SheetRaj/go-broker/internal/config" // Update with your actual path
	"github.com/SheetRaj/go-broker/internal/network"
	"github.com/SheetRaj/go-broker/internal/storage"
)

func main() {

	cfg, _ := config.Load("config.json")

	// 1. Storage Engine
	wal, err := storage.NewWAL(cfg.Storage.DataDir, "orders")
	if err != nil {
		log.Fatalf("WAL Error: %v", err)
	}
	defer wal.Close()

	// 2. NEW: Offset Manager
	om := storage.NewOffsetManager(cfg.Storage.DataDir)

	// 3. Listener
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("Bind failed: %v", err)
	}
	fmt.Println("Server Listening on :9000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		// Pass 'om' to the handler
		go network.HandleConnection(conn, wal, om)
	}
}
