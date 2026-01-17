package main

import (
	"log"
	"net"

	"google.golang.org/grpc"

	v1 "github.com/bachtg/distributed_sys/api/v1"
	"github.com/bachtg/distributed_sys/internal/server"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	v1.RegisterPingServiceServer(
		grpcServer,
		server.NewPingServer(),
	)

	log.Println("gRPC server listening on :50051")
	log.Fatal(grpcServer.Serve(lis))
}
