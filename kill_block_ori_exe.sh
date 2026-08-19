#!/bin/bash

# Script to kill all processes started by "block_ori_exe"
# Usage: ./kill_block_ori_exe.sh [signal]

# Default signal is SIGTERM (15)
SIGNAL=${1:-15}

echo "Looking for processes started by 'block_ori_exe'..."

# Find all processes with "block_ori_exe" in their command line
# Using pgrep to get PIDs of processes matching "block_ori_exe"
PIDS=$(pgrep -f "block_ori_exe")

if [ -z "$PIDS" ]; then
    echo "No processes found with 'block_ori_exe' in their command line."
    exit 0
fi

echo "Found the following processes:"
ps -fp $PIDS

echo ""
echo "Sending signal $SIGNAL to processes: $PIDS"

# Kill the processes
kill -$SIGNAL $PIDS 2>/dev/null

# Check if any processes are still running
sleep 1
STILL_RUNNING=$(pgrep -f "block_ori_exe")

if [ -n "$STILL_RUNNING" ]; then
    echo "Some processes are still running. Sending SIGKILL (9)..."
    kill -9 $STILL_RUNNING 2>/dev/null

    # Final check
    sleep 0.5
    FINAL_RUNNING=$(pgrep -f "block_ori_exe")

    if [ -n "$FINAL_RUNNING" ]; then
        echo "Warning: Some processes may still be running: $FINAL_RUNNING"
        echo "You may need to run as root or check process permissions."
        exit 1
    else
        echo "All processes have been terminated."
    fi
else
    echo "All processes have been terminated."
fi

exit 0
