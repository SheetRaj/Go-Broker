package network

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/SheetRaj/go-broker/internal/storage"
)

// HandleConnection manages one client connection
func HandleConnection(conn net.Conn, wal *storage.WAL, om *storage.OffsetManager) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// 1. Read the command from the client
		line, err := reader.ReadString('\n')
		if err != nil {
			break // client disconnected
		}

		// 2. Parse the command
		line = strings.TrimSpace(line)
		parts := strings.Split(line, " ")
		cmd := parts[0]

		switch cmd {
		case "PUB":
			// FORMAT: PUB <topic> <length>
			// Example: PUB orders 5
			if len(parts) < 3 {
				conn.Write([]byte("ERR Invalid PUB args\n"))
				continue
			}

			// Parse length
			length, _ := strconv.Atoi(parts[2])

			// Read the exact number of bytes for the payload
			payload := make([]byte, length)
			_, err := reader.Read(payload)
			if err != nil {
				conn.Write([]byte("ERR: Failed to read payload\n"))
				break
			}
			// Clean up any trailing newline after the payload
			reader.ReadByte() // consume \r
			reader.ReadByte() // consume \n

			// Write to WAL
			offset, err := wal.Write(payload)
			if err != nil {
				conn.Write([]byte("ERR: Disk write failed\n"))
			} else {
				response := fmt.Sprintf("OK %d\n", offset)
				conn.Write([]byte(response))
			}
			fmt.Printf("Received: %s (Offset: %d)\n", string(payload), offset)

		case "READ":
			// FORMAT: READ <offset>
			if len(parts) < 2 {
				conn.Write([]byte("ERR: Invalid READ args\n"))
				continue
			}
			offset, _ := strconv.ParseInt(parts[1], 10, 64)
			data, err := wal.Read(offset)
			if err != nil {
				conn.Write([]byte("ERR: Disk read failed\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("MSG %s\n", string(data))))
			}
		case "ACK":
			// FORMAT: ACK <group> <offset>
			// Example: ACK worker1 18
			if len(parts) < 3 {
				continue
			}
			group := parts[1]
			offset, _ := strconv.Atoi(parts[2])

			om.SaveOffset(group, offset)
			conn.Write([]byte("OK\n"))
			fmt.Printf("ACK: %s is now at %d\n", group, offset)

		case "OFFSET":
			// FORMAT: OFFSET <group>
			// Example: OFFSET worker1
			if len(parts) < 2 {
				continue
			}
			group := parts[1]
			savedOffset := om.GetOffset(group)
			conn.Write([]byte(fmt.Sprintf("%d\n", savedOffset)))
		default:
			conn.Write([]byte("ERR: Unknown command\n"))
		}
	}
}
