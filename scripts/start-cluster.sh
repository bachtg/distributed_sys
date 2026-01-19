#!/bin/bash

# Script để start cluster một cách đơn giản

CONFIG_FILE=${1:-"config/cluster.json"}

# Parse số lượng node từ config file
NODE_COUNT=$(jq '.nodes | length' "$CONFIG_FILE")

echo "Starting $NODE_COUNT nodes from $CONFIG_FILE"

# Tạo thư mục data nếu chưa tồn tại
mkdir -p data

# Khởi động từng node
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_ID=$(jq -r ".nodes[$i].node_id" "$CONFIG_FILE")
    
    echo "Starting node: $NODE_ID"
    
    # Chạy trong background và redirect output to log file
    ./bin/raft-server -node-id "$NODE_ID" -config "$CONFIG_FILE" \
        > "logs/$NODE_ID.log" 2>&1 &
    
    PID=$!
    echo "Node $NODE_ID started with PID: $PID"
    
    # Lưu PID để shutdown sau này
    echo $PID > "pids/$NODE_ID.pid"
    
    # Đợi một chút trước khi start node tiếp theo
    if [ $i -eq 0 ]; then
        echo "Waiting for bootstrap node to be ready..."
        sleep 2
    else
        sleep 1
    fi
done

echo "All nodes started. Check logs in logs/ directory"
echo "To stop all nodes, run: ./stop-cluster.sh"