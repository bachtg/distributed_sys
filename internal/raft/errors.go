package raft

import "errors"

var (
	ErrorNotLeader      = errors.New("requested node is not the leader")
	ErrSnapshotNotFound = errors.New("snapshot not found")
)
