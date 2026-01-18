package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/bachtg/distributed_sys/internal/domain"
)

type FiniteStateMachine struct {
	mu        sync.RWMutex
	commitLog *domain.CommitLog
}

type fsmSnapshot struct{}

func NewFiniteStateMachine(dataDir string) (*FiniteStateMachine, error) {
	cl, err := domain.NewCommitLog(filepath.Join(dataDir, "commitlog"))
	if err != nil {
		return nil, err
	}

	return &FiniteStateMachine{
		commitLog: cl,
	}, nil
}

// Apply applies a Raft log entry to the FSM
func (fsm *FiniteStateMachine) Apply(raftLog *raft.Log) interface{} {
	var logEntry domain.LogEntry
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
func (fsm *FiniteStateMachine) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

// Restore restores the FSM from a snapshot
// NOT IMPLEMENTED
func (fsm *FiniteStateMachine) Restore(rc io.ReadCloser) error {
	return nil
}

// NOT IMPLEMENTED
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

// NOT IMPLEMENTED
func (f *fsmSnapshot) Release() {}
