package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/bachtg/distributed_sys/internal/consensus/raft"
)

type Server struct {
	*raft.RaftNode
	httpServer *http.Server
}

func NewServer(raftNode *raft.RaftNode, httpAddr string) *Server {
	server := &Server{
		RaftNode: raftNode,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/append", server.handleAppend)
	mux.HandleFunc("/read", server.handleRead)
	mux.HandleFunc("/join", server.handleJoin)
	mux.HandleFunc("/stats", server.handleStats)

	server.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	return server
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Data string `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	entry, err := s.RaftNode.Append(req.Data)
	if err != nil {
		if err.Error() == raft.ErrorNotLeader.Error() {
			leader := s.RaftNode.GetLeader()
			w.Header().Set("X-Leader", leader)
			http.Error(w, fmt.Sprintf("not leader, try %s", leader), http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	offsetStr := r.URL.Query().Get("offset")
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	entry, err := s.RaftNode.Read(offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entry)
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		NodeID string `json:"node_id"`
		Addr   string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.RaftNode.Join(req.NodeID, req.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"is_leader": s.RaftNode.IsLeader(),
		"leader":    s.RaftNode.GetLeader(),
		"node_id":   s.RaftNode.GetNodeID(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}
