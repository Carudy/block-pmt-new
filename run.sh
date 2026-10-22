#!/bin/bash

# Basic Blockchain Shard Network Runner

# 中断 / 挂断 时清理节点进程，避免遗留进程占用端口（Ctrl-C / kill / 关闭终端）
trap './kill_block_exe.sh >/dev/null 2>&1 || true; exit 130' INT TERM HUP

./kill_block_exe.sh  # Kill any existing block_exe processes
./clean.sh

# ==== 期望运行的方案（会据此自动改写 params/config.go 的 Schema 行）：
# lb | ftl | caravan | preempt ====
# 可被环境变量 SCHEMA 覆盖（run_exp.sh 批量实验用）： SCHEMA=ftl ./run.sh
SCHEMA="${SCHEMA:-lb}"

# rebuild（先按 SCHEMA 改写 config.go，再编译）
./set_config.sh --schema "$SCHEMA" || { echo "set_config.sh 失败，终止。"; exit 1; }
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
num_shards="${NUM_SHARDS:-2}"      # Number of shards (S0, S1, S2, S3)
num_nodes="${NUM_NODES:-16}"       # Number of nodes per shard (this branch runs one node per shard)
malicious_num="${MALICIOUS_NUM:-1}" # Maximum malicious nodes per shard
test_file="${TEST_FILE:-20W.csv}"    # Test file

# 自动挑选未被占用的端口基址（可用 PORT_BASE 环境变量覆盖）
if [ -z "${PORT_BASE:-}" ]; then
    # 通过“真正尝试 bind”来判定：对任何进程占用、以及临时端口(ephemeral)冲突都有效
    PORT_BASE=$(./block_exe --find-port-base -S "$num_shards" -N "$num_nodes" 2>/dev/null)
    if [ -z "$PORT_BASE" ]; then
        echo "ERROR: 找不到可用端口段（需要跨度约 $((num_shards*num_nodes+1)) 个端口）。"
        echo "       可先运行 ./kill_block_exe.sh 清理遗留进程，或用 PORT_BASE=xxxxx ./run.sh 手动指定。"
        exit 1
    fi
fi
export PORT_BASE

echo "=== Starting Blockchain Shard Network ==="
echo "Shards: $num_shards"
echo "Nodes per shard: $num_nodes"
echo "Malicious nodes per shard: $malicious_num"
echo "Port base: $PORT_BASE"
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
# RUN_TIMEOUT_SEC>0 时，超时即终止本次运行（避免严格 nonce 模式卡死后无人干预）。
echo "Waiting for the run to finish (client prints '停止时间') ..."
run_timeout="${RUN_TIMEOUT_SEC:-0}"
run_started=$(date +%s)
while ! grep -q "停止时间" "$client_log" 2>/dev/null; do
    if ! kill -0 "$client_pid" 2>/dev/null; then
        echo "client 进程已退出（未检测到 '停止时间'，可能异常结束）。"
        break
    fi
    if [ "$run_timeout" -gt 0 ] && [ $(( $(date +%s) - run_started )) -ge "$run_timeout" ]; then
        echo "!! 超过 RUN_TIMEOUT_SEC=${run_timeout}s 仍未结束，终止本次运行。" >&2
        ./kill_block_exe.sh
        exit 2
    fi
    sleep 5
done

# 给各个节点一点时间把日志刷盘
sleep 2

echo ""
echo "Run finished. Stopping nodes ..."
./kill_block_exe.sh

echo "Packaging results ..."
if [ "${NO_PACK:-0}" = "1" ]; then
    echo "NO_PACK=1：跳过打包，日志保留在 ./log/（smoke_test.sh 使用）"
else
    N_SHARD="$num_shards" N_NODES="$num_nodes" ./zip_res.sh
fi
