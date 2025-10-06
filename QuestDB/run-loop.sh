#!/bin/bash

echo "[INFO] Starting run-loop + QuestDB..."

# Start QuestDB in the background
/docker-entrypoint.sh &

# Wait for QuestDB to be ready on port 9000
until curl -s http://localhost:9000/ > /dev/null; do
    sleep 1
done

echo "[INFO] QuestDB is ready. Starting main loop."

# Run your app every 30 seconds
while true; do
    echo "[INFO] Running ./build/main at $(date)"
    /app/build/main
    sleep 30
done
