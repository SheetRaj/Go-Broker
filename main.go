package main

import (
	"fmt"
	"log"

	"github.com/SheetRaj/go-broker/internal/config"
	"github.com/SheetRaj/go-broker/internal/storage"
)

func main() {
	fmt.Println("Phase 1.5: Indexing & Reading Test...")

	cfg, err := config.Load("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	wal, err := storage.NewWAL(cfg.Storage.DataDir, "orders")
	if err != nil {
		log.Fatalf("Failed to create WAL: %v", err)
	}
	defer wal.Close()

	// --- WRITE PHASE ---
	fmt.Println("\nWriting Data...")

	// We verify the index by storing the returned offsets in a simple map
	// In a real DB, this map would be the "Index"
	myIndex := make(map[int]int64)

	offset1, _ := wal.Write([]byte("Message One"))
	myIndex[1] = offset1
	fmt.Printf("Msg 1 stored at Offset: %d\n", offset1)

	offset2, _ := wal.Write([]byte("Message Two (Longer)"))
	myIndex[2] = offset2
	fmt.Printf("Msg 2 stored at Offset: %d\n", offset2)

	offset3, _ := wal.Write([]byte("Msg 3"))
	myIndex[3] = offset3
	fmt.Printf("Msg 3 stored at Offset: %d\n", offset3)

	// --- READ PHASE ---
	fmt.Println("\nReading Data Randomly...")

	// Let's try to fetch Message 2 directly without reading Msg 1 or 3
	targetOffset := myIndex[2]
	data, err := wal.Read(targetOffset)
	if err != nil {
		log.Fatalf("Read error: %v", err)
	}

	fmt.Printf("Read at Offset %d -> Payload: '%s'\n", targetOffset, string(data))

	if string(data) == "Message Two (Longer)" {
		fmt.Println("\nSuccess! Random Access works.")
	} else {
		fmt.Println("\nTest Failed: Data mismatch.")
	}
}
