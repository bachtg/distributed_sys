package main

import (
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/bachtg/distributed_sys/internal/cluster"
)

func main() {
	// Parse command line flags
	nodeID := flag.String("node-id", "", "Node ID (required)")
	configPath := flag.String("config", "config/cluster.json", "Path to cluster config file")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node-id is required")
	}

	// Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load cluster configuration
	config, err := cluster.LoadClusterConfig(*configPath)
	if err != nil {
		logger.Error("Failed to load cluster config", "error", err)
		os.Exit(1)
	}

	// Create cluster manager
	manager := cluster.NewClusterManager(logger, config)

	// Start node in a goroutine
	go func() {
		if err := manager.StartNode(*nodeID); err != nil {
			logger.Error("Failed to start node", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down node", "node_id", *nodeID)
}
