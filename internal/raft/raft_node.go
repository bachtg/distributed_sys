package raft

import (
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"github.com/bachtg/distributed_sys/internal/commit_log"
)

type RaftNode struct {
	logger             *slog.Logger
	raft               *raft.Raft
	finiteStateMachine *finiteStateMachine
	transport          *raft.NetworkTransport
	config             *RaftNodeConfig
	logStore           *raftboltdb.BoltStore
	snapshotStore      *raft.FileSnapshotStore
}

type RaftNodeConfig struct {
	NodeId            string
	NodeAddress       string
	DataDir           string
	Bootstrap         bool
	SnapshotInterval  time.Duration // Interval giữa các snapshot
	SnapshotThreshold uint64        // Số log entries trước khi trigger snapshot
	SnapshotRetention int           // Số snapshot giữ lại
}

func NewRaftNode(logger *slog.Logger, config *RaftNodeConfig) (*RaftNode, error) {
	// Tạo data directory nếu chưa tồn tại
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, err
	}

	// Auto-detect bootstrap: nếu data dir rỗng -> bootstrap node
	isEmpty, err := isDataDirEmpty(config.DataDir)
	if err != nil {
		return nil, err
	}

	// Override bootstrap flag nếu data dir rỗng
	if isEmpty && !config.Bootstrap {
		logger.Info("Data directory is empty, auto-enabling bootstrap mode",
			"node_id", config.NodeId,
			"data_dir", config.DataDir)
		config.Bootstrap = true
	}

	// Nếu data dir không rỗng nhưng có flag bootstrap, log warning
	if !isEmpty && config.Bootstrap {
		logger.Warn("Data directory is not empty but bootstrap flag is set, will try to recover from existing data",
			"node_id", config.NodeId,
			"data_dir", config.DataDir)
		config.Bootstrap = false
	}

	// Initialize finite state machine
	fsm, err := newFiniteStateMachine(logger, config.DataDir)
	if err != nil {
		return nil, err
	}

	// Setup TCP transport
	addr, err := net.ResolveTCPAddr(Raft_Communication_Protocol, config.NodeAddress)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(
		config.NodeAddress,
		addr,
		Raft_Communication_MaxPool,
		Raft_Communication_Timeout,
		os.Stderr,
	)
	if err != nil {
		return nil, err
	}

	// Setup log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, RaftNode_FileName))
	if err != nil {
		return nil, err
	}

	// Setup snapshot store with retention
	snapshotRetention := config.SnapshotRetention
	if snapshotRetention == 0 {
		snapshotRetention = 3 // Default: giữ 3 snapshot
	}

	snapshotStore, err := raft.NewFileSnapshotStore(config.DataDir, snapshotRetention, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Configure Raft
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeId)

	// Snapshot configuration
	if config.SnapshotInterval > 0 {
		raftConfig.SnapshotInterval = config.SnapshotInterval
	} else {
		raftConfig.SnapshotInterval = 120 * time.Second // Default: 2 phút
	}

	if config.SnapshotThreshold > 0 {
		raftConfig.SnapshotThreshold = config.SnapshotThreshold
	} else {
		raftConfig.SnapshotThreshold = 8192 // Default: 8192 log entries
	}

	// Trailing logs (số log entries giữ lại sau snapshot)
	raftConfig.TrailingLogs = 1024

	logger.Info("Raft configuration",
		"snapshot_interval", raftConfig.SnapshotInterval,
		"snapshot_threshold", raftConfig.SnapshotThreshold,
		"trailing_logs", raftConfig.TrailingLogs,
	)

	// Create Raft instance
	raftInstance, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	// Bootstrap cluster nếu cần
	if config.Bootstrap {
		logger.Info("Bootstrapping cluster", "node_id", config.NodeId)
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		f := raftInstance.BootstrapCluster(configuration)
		if err := f.Error(); err != nil {
			logger.Error("Failed to bootstrap cluster", "error", err)
			return nil, err
		}
		logger.Info("Cluster bootstrapped successfully", "node_id", config.NodeId)
	}

	node := &RaftNode{
		logger:             logger,
		raft:               raftInstance,
		finiteStateMachine: fsm,
		config:             config,
		transport:          transport,
		logStore:           logStore,
		snapshotStore:      snapshotStore,
	}

	// Start snapshot monitoring
	go node.monitorSnapshots()

	return node, nil
}

// isDataDirEmpty kiểm tra xem data directory có rỗng không
func isDataDirEmpty(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}

	// Kiểm tra các file quan trọng của Raft
	hasRaftData := false
	for _, entry := range entries {
		name := entry.Name()
		// Kiểm tra các file/folder của Raft
		if name == RaftNode_FileName || name == "snapshots" || name == "raft.db" {
			hasRaftData = true
			break
		}
	}

	return !hasRaftData, nil
}

// monitorSnapshots theo dõi và log thông tin về snapshots
func (rn *RaftNode) monitorSnapshots() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		snapshots, err := rn.snapshotStore.List()
		if err != nil {
			rn.logger.Error("Failed to list snapshots", "error", err)
			continue
		}

		if len(snapshots) > 0 {
			rn.logger.Debug("Snapshot status",
				"count", len(snapshots),
				"latest_index", snapshots[0].Index,
				"latest_term", snapshots[0].Term,
			)
		}
	}
}

// TriggerSnapshot force tạo snapshot ngay lập tức
func (rn *RaftNode) TriggerSnapshot() error {
	rn.logger.Info("Triggering manual snapshot")
	future := rn.raft.Snapshot()
	if err := future.Error(); err != nil {
		rn.logger.Error("Failed to create snapshot", "error", err)
		return err
	}
	rn.logger.Info("Manual snapshot created successfully")
	return nil
}

// GetSnapshotStats trả về thông tin về snapshots
func (rn *RaftNode) GetSnapshotStats() (map[string]interface{}, error) {
	snapshots, err := rn.snapshotStore.List()
	if err != nil {
		return nil, err
	}

	stats := map[string]interface{}{
		"count": len(snapshots),
	}

	if len(snapshots) > 0 {
		latest := snapshots[0]
		stats["latest_index"] = latest.Index
		stats["latest_term"] = latest.Term
		stats["latest_size"] = latest.Size
	}

	return stats, nil
}

// RestoreFromSnapshot khôi phục state từ snapshot (for testing/recovery)
func (rn *RaftNode) RestoreFromSnapshot(snapshotID string) error {
	rn.logger.Info("Restoring from snapshot", "snapshot_id", snapshotID)

	snapshots, err := rn.snapshotStore.List()
	if err != nil {
		return err
	}

	for _, snapshot := range snapshots {
		if snapshot.ID == snapshotID {
			_, reader, err := rn.snapshotStore.Open(snapshot.ID)
			if err != nil {
				return err
			}
			defer reader.Close()

			// Read snapshot data
			data, err := io.ReadAll(reader)
			if err != nil {
				return err
			}

			rn.logger.Info("Snapshot restored", "size", len(data))
			return nil
		}
	}

	return ErrSnapshotNotFound
}

func (rn *RaftNode) Append(data string) (commit_log.LogEntry, error) {
	rn.logger.Info("Append called", "data", data, "node_id", rn.config.NodeId)
	defer rn.logger.Info("Append finished", "node_id", rn.config.NodeId)

	if rn.raft.State() != raft.Leader {
		return commit_log.LogEntry{}, ErrorNotLeader
	}

	logEntry := commit_log.LogEntry{
		Timestamp: time.Now(),
		Data:      data,
	}

	logEntryBytes, err := json.Marshal(logEntry)
	if err != nil {
		return commit_log.LogEntry{}, err
	}

	future := rn.raft.Apply(logEntryBytes, RaftNode_TimeoutAction_APPLY)
	if err := future.Error(); err != nil {
		return commit_log.LogEntry{}, err
	}

	result := future.Response()
	if err, ok := result.(error); ok {
		return commit_log.LogEntry{}, err
	}

	return result.(commit_log.LogEntry), nil
}

func (rn *RaftNode) Read(offset int64) (commit_log.LogEntry, error) {
	rn.finiteStateMachine.mu.RLock()
	defer rn.finiteStateMachine.mu.RUnlock()

	return rn.finiteStateMachine.commitLog.Read(offset)
}

func (rn *RaftNode) Join(nodeID, addr string) error {
	if rn.raft.State() != raft.Leader {
		return ErrorNotLeader
	}

	configFuture := rn.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			rn.logger.Info("Node already in cluster", "node_id", nodeID)
			return nil
		}
	}

	rn.logger.Info("Adding node to cluster", "node_id", nodeID, "addr", addr)
	future := rn.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(addr),
		0,
		0,
	)

	if err := future.Error(); err != nil {
		rn.logger.Error("Failed to add node", "node_id", nodeID, "error", err)
		return err
	}

	rn.logger.Info("Node added successfully", "node_id", nodeID)
	return nil
}

func (rn *RaftNode) GetLeader() string {
	return string(rn.raft.Leader())
}

func (rn *RaftNode) IsLeader() bool {
	return rn.raft.State() == raft.Leader
}

func (rn *RaftNode) GetState() string {
	return rn.raft.State().String()
}

func (rn *RaftNode) GetStats() map[string]string {
	return rn.raft.Stats()
}

func (rn *RaftNode) Close() error {
	rn.logger.Info("Shutting down Raft node", "node_id", rn.config.NodeId)

	future := rn.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	if err := rn.logStore.Close(); err != nil {
		return err
	}

	return rn.finiteStateMachine.commitLog.Close()
}

func (rn *RaftNode) GetNodeID() string {
	return rn.config.NodeId
}
