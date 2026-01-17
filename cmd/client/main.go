package main

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"

	v1 "github.com/bachtg/distributed_sys/api/v1"
)

const (
	serverAddr = "server:50051"
	timeout    = time.Second
	maxRetry   = 3
)

func callPing(
	wg *sync.WaitGroup,
	id int,
	client v1.PingServiceClient,
) {
	defer wg.Done()

	reqID := time.Now().UnixNano()

	for attempt := 1; attempt <= maxRetry; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		resp, err := client.Ping(ctx, &v1.PingRequest{
			Message: "hello",
		})
		cancel()

		if err == nil {
			log.Printf(
				"[req=%d] success attempt=%d latency=%s response=%s",
				reqID,
				attempt,
				time.Since(start),
				resp.Message,
			)
			return
		}

		log.Printf(
			"[req=%d] failed attempt=%d err=%v",
			reqID,
			attempt,
			err,
		)

		time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
	}

	log.Printf("[req=%d] all retries failed", reqID)
}

func main() {
	conn, err := grpc.Dial(
		serverAddr,
		grpc.WithInsecure(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := v1.NewPingServiceClient(conn)

	var wg sync.WaitGroup
	total := 5

	for i := 0; i < total; i++ {
		wg.Add(1)
		go callPing(&wg, i, client)
	}

	wg.Wait()
	log.Println("client done")
}
