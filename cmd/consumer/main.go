package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:9000")
	reader := bufio.NewReader(conn)

	// CONSUMER GROUP NAME
	myGroup := "worker1"

	// 1. RECOVERY: Ask Server for last position
	fmt.Fprintf(conn, "OFFSET %s\n", myGroup)
	resp, _ := reader.ReadString('\n')
	currentOffset, _ := strconv.Atoi(strings.TrimSpace(resp))

	fmt.Printf("Resuming from Offset: %d\n", currentOffset)

	for {
		// 2. Fetch
		fmt.Fprintf(conn, "READ %d\n", currentOffset)
		response, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		response = strings.TrimSpace(response)

		if strings.HasPrefix(response, "MSG ") {
			msg := strings.TrimPrefix(response, "MSG ")
			fmt.Printf("Processed: %s\n", msg)

			// 3. Math (Next Offset)
			length := len(msg)
			currentOffset += 4 + length // Header + Data

			// 4. CRITICAL: ACK (Save Progress)
			fmt.Fprintf(conn, "ACK %s %d\n", myGroup, currentOffset)
			// Read the OK from server to keep protocol sync
			reader.ReadString('\n')
		} else {
			time.Sleep(1 * time.Second)
		}
	}
}
