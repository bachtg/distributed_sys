package server

import (
	"context"

	v1 "github.com/bachtg/distributed_sys/api/v1"
)

type PingServer struct {
	v1.UnimplementedPingServiceServer
}

func NewPingServer() *PingServer {
	return &PingServer{}
}

func (s *PingServer) Ping(ctx context.Context, request *v1.PingRequest) (*v1.PingResponse, error) {
	return &v1.PingResponse{
		Message: "pong: " + request.Message,
	}, nil
}
