package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/bachtg/distributed_sys/internal/commit_log"
)

type finiteStateMachine struct {
	logger    *slog.Logger
	mu        sync.RWMutex
	commitLog *commit_log.CommitLog
}

type fsmSnapshot struct{}

func newFiniteStateMachine(logger *slog.Logger, dataDir string) (*finiteStateMachine, error) {
	cl, err := commit_log.NewCommitLog(logger, filepath.Join(dataDir, "commitlog"))
	if err != nil {
		return nil, err
	}

	return &finiteStateMachine{
		logger:    logger,
		commitLog: cl,
	}, nil
}

// Apply applies a Raft log entry to the FSM
func (fsm *finiteStateMachine) Apply(raftLog *raft.Log) interface{} {
	fsm.logger.Info("FSM | Applying | Start processing", "index", raftLog.Index, "data", string(raftLog.Data))
	defer fsm.logger.Info("FSM | Applying | Finished processing", "index", raftLog.Index)

	var logEntry commit_log.LogEntry
	if err := json.Unmarshal(raftLog.Data, &logEntry); err != nil {
		return fmt.Errorf("failed to unmarshal: %v", err)
	}

	// use Raft log index as offset to ensure consistency across replicas
	logEntry.Offset = int64(raftLog.Index)

	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if err := fsm.commitLog.AppendAtOffset(logEntry); err != nil {
		return err
	}

	return logEntry
}

// Snapshot returns an FSM snapshot
// NOT IMPLEMENTED
func (fsm *finiteStateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

// Restore restores the FSM from a snapshot
// NOT IMPLEMENTED
func (fsm *finiteStateMachine) Restore(rc io.ReadCloser) error {
	return nil
}

// NOT IMPLEMENTED
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// NOT IMPLEMENTED
func (f *fsmSnapshot) Release() {}
