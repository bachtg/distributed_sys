package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bachtg/distributed_sys/internal/consensus/raft"
	server "github.com/bachtg/distributed_sys/internal/server/http"
)

var logger *slog.Logger

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <node1|node2|node3>")
		os.Exit(1)
	}

	nodeType := os.Args[1]

	var config raft.RaftNodeConfig
	var httpAddr string

	switch nodeType {
	case "node1":
		config = raft.RaftNodeConfig{
			NodeId:      "node1",
			NodeAddress: "localhost:7001",
			DataDir:     "./data/node1",
			Bootstrap:   true,
		}
		httpAddr = ":8001"

	case "node2":
		config = raft.RaftNodeConfig{
			NodeId:      "node2",
			NodeAddress: "localhost:7002",
			DataDir:     "./data/node2",
			Bootstrap:   false,
		}
		httpAddr = ":8002"

	case "node3":
		config = raft.RaftNodeConfig{
			NodeId:      "node3",
			NodeAddress: "localhost:7003",
			DataDir:     "./data/node3",
			Bootstrap:   false,
		}
		httpAddr = ":8003"

	default:
		log.Fatal("Unknown node type. Use: node1, node2, or node3")
	}

	// new logger
	logger = slog.Default()

	// create new raft node
	raftNode, err := raft.NewRaftNode(logger, &config)
	if err != nil {
		log.Fatal(err)
	}
	defer raftNode.Close()

	// If not bootstrap node, join cluster after delay
	if !config.Bootstrap {
		go func() {
			time.Sleep(3 * time.Second)
			joinReq := map[string]string{
				"node_id": config.NodeId,
				"addr":    config.NodeAddress,
			}
			reqBody, _ := json.Marshal(joinReq)

			resp, err := http.Post(
				"http://localhost:8001/join",
				"application/json",
				strings.NewReader(string(reqBody)),
			)
			if err != nil {
				log.Printf("Failed to join cluster: %v", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				log.Printf("Successfully joined cluster")
			} else {
				log.Printf("Failed to join cluster: status %d", resp.StatusCode)
			}
		}()
	}

	// Start HTTP server
	server := server.NewServer(raftNode, httpAddr)
	log.Printf("Starting node %s on %s (Raft: %s)", config.NodeId, httpAddr, config.NodeAddress)
	log.Fatal(server.Start())
}
