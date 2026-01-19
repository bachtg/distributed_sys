#!/bin/bash

# Script để stop tất cả các node trong cluster

PID_DIR="pids"

if [ ! -d "$PID_DIR" ]; then
    echo "No PID directory found. Cluster may not be running."
    exit 0
fi

echo "Stopping all nodes..."

for pid_file in "$PID_DIR"/*.pid; do
    if [ -f "$pid_file" ]; then
        NODE_ID=$(basename "$pid_file" .pid)
        PID=$(cat "$pid_file")
        
        if kill -0 "$PID" 2>/dev/null; then
            echo "Stopping node $NODE_ID (PID: $PID)"
            kill "$PID"
            
            # Wait for process to stop
            timeout 10 bash -c "while kill -0 $PID 2>/dev/null; do sleep 0.5; done"
            
            if kill -0 "$PID" 2>/dev/null; then
                echo "Force killing node $NODE_ID"
                kill -9 "$PID"
            fi
        else
            echo "Node $NODE_ID is not running"
        fi
        
        rm "$pid_file"
    fi
done

echo "All nodes stopped"