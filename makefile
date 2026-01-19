.PHONY: build clean start stop restart logs status

# Build binary
build:
	@echo "Building raft-server..."
	@mkdir -p bin
	@go build -o bin/raft-server cmd/main.go

# Clean all data and logs
clean:
	@echo "Cleaning data and logs..."
	@rm -rf data/* logs/* pids/*

# Start cluster
start: build
	@echo "Starting cluster..."
	@mkdir -p logs pids
	@chmod +x scripts/start-cluster.sh
	@./scripts/start-cluster.sh config/cluster.json

# Stop cluster
stop:
	@echo "Stopping cluster..."
	@chmod +x scripts/stop-cluster.sh
	@./scripts/stop-cluster.sh

# Restart cluster
restart: stop clean start

# View logs
logs:
	@tail -f logs/*.log

# Check cluster status
status:
	@echo "Checking cluster status..."
	@for port in 8001 8002 8003; do \
		echo "Node on port $$port:"; \
		curl -s http://127.0.0.1:$$port/stats | jq . || echo "Not responding"; \
		echo ""; \
	done

# Start with Docker
docker-start:
	@docker-compose up --build -d

# Stop Docker containers
docker-stop:
	@docker-compose down

# View Docker logs
docker-logs:
	@docker-compose logs -f

# Run single node for testing
run-node:
	@go run cmd/main.go -node-id $(NODE_ID) -config config/cluster.json