package raft

import "errors"

var (
	ErrorNotLeader = errors.New("requestted node is not the leader")
)
