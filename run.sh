#!/bin/bash

# Basic Blockchain Shard Network Runner
# Sets num_shards and num_nodes variables and starts all nodes
./kill_block_exe.sh  # Kill any existing block_exe processes
./clean.sh

# ==== 期望运行的方案（会据此自动改写 params/config.go 的 Schema 行）：
# lb | ftl | caravan | preempt ====
SCHEMA=lb

# rebuild（先按 SCHEMA 改写 config.go，再编译）
./set_schema.sh "$SCHEMA" || { echo "set_schema.sh 失败，终止。"; exit 1; }
go build -o block_exe .

# --- 从 params/config.go 检测当前 scheme 与注入速率 ---
source ./schema_detect.sh
if schema_detect; then
    echo "Detected scheme : ${SCHEMA}"
    echo "Inject_speed    : ${INJECT_SPEED}"
else
    echo "WARN: 无法从 params/config.go 识别 scheme（请确认 Schema 行未被注释）"
fi
echo ""

# Configuration variables - edit these as needed
num_shards=2      # Number of shards (S0, S1, S2, S3)
num_nodes=16       # Number of nodes per shard (this branch runs one node per shard)
malicious_num=1   # Maximum malicious nodes per shard
test_file="20W.csv"  # Test file

echo "=== Starting Blockchain Shard Network ==="
echo "Shards: $num_shards"
echo "Nodes per shard: $num_nodes"
echo "Malicious nodes per shard: $malicious_num"
echo "Test file: $test_file"
echo ""

# Check if block_exe exists
if [ ! -f "./block_exe" ]; then
    echo "ERROR: block_exe executable not found!"
    echo "Please build the project first: go build -o block_exe"
    exit 1
fi

# Check if test file exists
if [ ! -f "$test_file" ]; then
    echo "ERROR: Test file '$test_file' not found!"
    exit 1
fi

# Make block_exe executable if needed
if [ ! -x "./block_exe" ]; then
    chmod +x ./block_exe
    echo "Made block_exe executable"
fi

# Create log directory
log_dir="./process_logs"
mkdir -p "$log_dir"
echo "Logs will be saved to: $log_dir/"
echo ""

# Array to store PIDs
node_pids=()

echo "Starting nodes..."
for ((s=0; s<num_shards; s++)); do
    shard_id="S$s"
    for ((n=0; n<num_nodes; n++)); do
        node_id="N$n"
        log_file="$log_dir/${shard_id}_${node_id}.log"

        echo "Starting $shard_id $node_id..."

        # Start node with exact command format from requirements
        ./block_exe -S $num_shards -N $num_nodes -s $shard_id -n $node_id -t $test_file > "$log_file" 2>&1 &

        pid=$!
        node_pids+=($pid)

        # Brief pause between node starts
        sleep 0.2

        echo "  Started (PID: $pid, Log: $log_file)"
    done
done

echo ""
echo "All nodes started!"
echo ""

# Start client/supervisor
echo "Starting client/supervisor..."
client_log="$log_dir/client.log"

# Start client with -c flag as requested
./block_exe -c -S $num_shards -N $num_nodes -t $test_file  > "$client_log" 2>&1 &

client_pid=$!
node_pids+=($client_pid)

sleep 1
echo "Client started (PID: $client_pid, Log: $client_log)"
echo ""

echo "=== Network is running ==="
echo "Total processes: ${#node_pids[@]}"
echo "All output is being logged to: $log_dir/"
echo ""
echo "To view logs:"
echo "  tail -f $log_dir/*.log"
echo ""

# --- 等待实验结束：client 在结束时打印 “停止时间：...” 并退出 ---
echo "Waiting for the run to finish (client prints '停止时间') ..."
while ! grep -q "停止时间" "$client_log" 2>/dev/null; do
    if ! kill -0 "$client_pid" 2>/dev/null; then
        echo "client 进程已退出（未检测到 '停止时间'，可能异常结束）。"
        break
    fi
    sleep 5
done

# 给各个节点一点时间把日志刷盘
sleep 2

echo ""
echo "Run finished. Stopping nodes ..."
./kill_block_exe.sh

echo "Packaging results ..."
N_SHARD="$num_shards" ./zip_res.sh
