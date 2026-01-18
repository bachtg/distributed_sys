package raft

import "time"

type RaftCommunication struct{}

const (
	Raft_Communication_Protocol = "tcp"
	Raft_Communication_Timeout  = 10 * time.Second
	Raft_Communication_MaxPool  = 3

	RaftNode_FileName            = "raft-log.db"
	RaftNode_TimeoutAction_APPLY = 10 * time.Second
)
