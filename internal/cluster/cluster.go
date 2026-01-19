package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/bachtg/distributed_sys/internal/raft"
	server "github.com/bachtg/distributed_sys/internal/server/http"
)

type ClusterManager struct {
	logger   *slog.Logger
	config   *ClusterConfig
	server   *server.Server
	raftNode *raft.RaftNode
}

func NewClusterManager(logger *slog.Logger, config *ClusterConfig) *ClusterManager {
	return &ClusterManager{
		logger: logger,
		config: config,
	}
}

func (cm *ClusterManager) StartNode(nodeId string) error {
	nodeConfig, err := cm.config.GetNodeConfig(nodeId)
	if err != nil {
		return err
	}

	cm.logger.Info("Starting node",
		"node_id", nodeId,
		"raft_addr", nodeConfig.RaftAddr,
		"data_dir", nodeConfig.DataDir)

	raftConfig := &raft.RaftNodeConfig{
		NodeId:            nodeConfig.NodeId,
		NodeAddress:       nodeConfig.RaftAddr,
		DataDir:           nodeConfig.DataDir,
		SnapshotInterval:  2 * time.Minute,
		SnapshotThreshold: 8192,
		SnapshotRetention: 3,
	}

	// Khởi tạo RaftNode
	raftNode, err := raft.NewRaftNode(cm.logger, raftConfig)
	if err != nil {
		return fmt.Errorf("failed to create raft node: %w", err)
	}

	cm.raftNode = raftNode

	// Khởi tạo HTTP server
	cm.server = server.NewServer(raftNode, nodeConfig.HTTPAddr)

	// Nếu có JoinAddr và node không phải bootstrap, tự động join vào cluster
	if nodeConfig.JoinAddr != "" {
		go cm.autoJoinCluster(nodeConfig)
	}

	// Khởi động HTTP server
	cm.logger.Info("HTTP server listening", "addr", nodeConfig.HTTPAddr)
	return cm.server.Start()
}

// autoJoinCluster tự động join node vào cluster
func (cm *ClusterManager) autoJoinCluster(nodeConfig *NodeConfig) {
	// Đợi một chút để node khởi động hoàn toàn
	time.Sleep(2 * time.Second)

	cm.logger.Info("Attempting to join cluster",
		"node_id", nodeConfig.NodeId,
		"join_addr", nodeConfig.JoinAddr)

	// Retry logic với exponential backoff
	maxRetries := 10
	backoff := 2 * time.Second

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			cm.logger.Info("Retrying join",
				"attempt", i+1,
				"backoff", backoff)
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}

		if err := cm.joinCluster(nodeConfig); err != nil {
			cm.logger.Error("Failed to join cluster",
				"error", err,
				"attempt", i+1)
			continue
		}

		cm.logger.Info("Successfully joined cluster",
			"node_id", nodeConfig.NodeId)
		return
	}

	cm.logger.Error("Failed to join cluster after max retries",
		"node_id", nodeConfig.NodeId)
}

// joinCluster gửi request join đến leader node
func (cm *ClusterManager) joinCluster(nodeConfig *NodeConfig) error {
	client := &http.Client{Timeout: 5 * time.Second}

	joinURL := fmt.Sprintf("http://%s/join", nodeConfig.JoinAddr)

	payload := map[string]string{
		"node_id": nodeConfig.NodeId,
		"addr":    nodeConfig.RaftAddr,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := client.Post(joinURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("join request failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

// Shutdown gracefully shuts down the cluster manager
func (cm *ClusterManager) Shutdown() error {
	if cm.raftNode != nil {
		return cm.raftNode.Close()
	}
	return nil
}
