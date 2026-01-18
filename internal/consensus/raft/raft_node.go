package raft

import (
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"

	"github.com/bachtg/distributed_sys/internal/domain"
)

type RaftNode struct {
	logger             slog.Logger
	raft               *raft.Raft
	finiteStateMachine *FiniteStateMachine
	transport          *raft.NetworkTransport
	config             *RaftNodeConfig
}

type RaftNodeConfig struct {
	NodeId      string
	NodeAddress string
	DataDir     string
	Bootstrap   bool
}

func NewRaftNode(logger *slog.Logger, config *RaftNodeConfig) (*RaftNode, error) {
	// finite state machine
	fsm, err := NewFiniteStateMachine(config.DataDir)
	if err != nil {
		return nil, err
	}

	// raft communicate using tcp protocol
	addr, err := net.ResolveTCPAddr(Raft_Communication_Protocol, config.NodeAddress)
	if err != nil {
		return nil, err
	}

	transport, err := raft.NewTCPTransport(config.NodeAddress, addr, Raft_Communication_MaxPool, Raft_Communication_Timeout, os.Stderr)
	if err != nil {
		return nil, err
	}

	// log store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, RaftNode_FileName))
	if err != nil {
		return nil, err
	}

	// snapshot store
	snapshotStore, err := raft.NewFileSnapshotStore(config.DataDir, 3, os.Stderr)
	if err != nil {
		return nil, err
	}

	// create raft instance
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeId)

	raftInstance, err := raft.NewRaft(raftConfig, fsm, logStore, logStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	if config.Bootstrap {
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
			return nil, err
		}
	}

	return &RaftNode{
		logger:             *logger,
		raft:               raftInstance,
		finiteStateMachine: fsm,
		config:             config,
		transport:          transport,
	}, nil
}

func (raftNode *RaftNode) Append(data string) (domain.LogEntry, error) {
	raftNode.logger.Info("Append called", "data", data, "node_id", raftNode.config.NodeId)
	defer raftNode.logger.Info("Append finished", "node_id", raftNode.config.NodeId)

	if raftNode.raft.State() != raft.Leader {
		return domain.LogEntry{}, ErrorNotLeader
	}

	logEntry := domain.LogEntry{
		Timestamp: time.Now(),
		Data:      data,
	}

	logEntryBytes, err := json.Marshal(logEntry)
	if err != nil {
		return domain.LogEntry{}, err
	}

	future := raftNode.raft.Apply(logEntryBytes, RaftNode_TimeoutAction_APPLY)
	if err := future.Error(); err != nil {
		return domain.LogEntry{}, err
	}

	result := future.Response()
	if err, ok := result.(error); ok {
		return domain.LogEntry{}, err
	}

	return result.(domain.LogEntry), nil
}

func (raftNode *RaftNode) Read(offset int64) (domain.LogEntry, error) {
	raftNode.finiteStateMachine.mu.RLock()
	defer raftNode.finiteStateMachine.mu.RUnlock()

	return raftNode.finiteStateMachine.commitLog.Read(offset)
}

func (raftNode *RaftNode) Join(nodeID, addr string) error {
	if raftNode.raft.State() != raft.Leader {
		return ErrorNotLeader
	}

	configFuture := raftNode.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(nodeID) {
			return nil
		}
	}

	future := raftNode.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(addr),
		0,
		0,
	)
	return future.Error()
}

func (raftNode *RaftNode) GetLeader() string {
	return string(raftNode.raft.Leader())
}

func (raftNode *RaftNode) IsLeader() bool {
	return raftNode.raft.State() == raft.Leader
}

func (raftNode *RaftNode) Close() error {
	future := raftNode.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}

	return raftNode.finiteStateMachine.commitLog.Close()
}

func (raftNode *RaftNode) GetNodeID() string {
	return raftNode.config.NodeId
}
