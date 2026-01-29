package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"
)

func main() {
	fmt.Println("👷 Starting Worker (Consumer)...")

	// 1. Connect to the Broker (The Server you are already running)
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Start at the beginning of the log
	currentOffset := 0

	// 2. The "Event Loop"
	for {
		// A. Ask for data at the current offset
		cmd := fmt.Sprintf("READ %d\n", currentOffset)
		conn.Write([]byte(cmd))

		// B. Read the Server's Response
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("❌ Server disconnected")
			return
		}

		response = strings.TrimSpace(response)

		// C. Handle "No Data" (Polling)
		if strings.HasPrefix(response, "ERR") {
			// No message yet? Wait 1 second and try again.
			time.Sleep(1 * time.Second)
			continue
		}

		// D. Handle Success (MSG payload)
		// Format: "MSG <payload>"
		if strings.HasPrefix(response, "MSG ") {
			// Extract the message part (remove "MSG " prefix)
			msg := strings.TrimPrefix(response, "MSG ")

			// Process the message (Simulate work)
			fmt.Printf("✅ Received at %d: %s\n", currentOffset, msg)

			// E. Calculate next offset
			// Next = Current + 4 (Header bytes) + Length of Data
			length := len(msg)
			currentOffset += 4 + length
		}
	}
}
