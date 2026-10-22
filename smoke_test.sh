#!/bin/bash
#
# smoke_test.sh — 改动后的快速体检。
#
# 用法：
#   ./smoke_test.sh                  # 快速检查（约 10s，无副作用）
#                                    #   gofmt(协议相关文件) + go build + 单元测试 + 脚本语法
#   ./smoke_test.sh --e2e            # 快速检查 + 一次端到端小实验（约 1-2 min）
#   SCHEMA=ftl ./smoke_test.sh --e2e # 指定 schema 跑 e2e（默认 preempt_escrow）
#   SMOKE_TXS=20000 ./smoke_test.sh --e2e    # e2e 的交易条数（默认 8000）
#   SMOKE_INJECT=1000 ./smoke_test.sh --e2e  # 注入速率（默认 200：让注入跨越迁移窗口，才会产生 batch）
#   SMOKE_TIMEOUT=1200 ./smoke_test.sh --e2e # e2e 超时（秒，默认 900）
#
# e2e 注意：
#   - 会执行 ./run.sh，因此会 kill 残留 block_exe，并清空 ./log/ ./record/
#     ./intermediate/ ./process_logs/（与正常跑实验一致；不会动 res/）。
#   - 通过 NO_PACK=1 跳过 zip_res.sh，不往 res/ 或 caddy 目录写任何东西。
#   - 结束时自动恢复 params/config.go。
#   - 若已有实验在跑，脚本会拒绝启动（SMOKE_FORCE=1 可强制）。
#
# 退出码：0 = 全部通过；1 = 有检查失败。

set -u

cd "$(dirname "$0")" || exit 1

usage() {
    cat <<'EOF'
用法: ./smoke_test.sh [--fast|--e2e|--help]

  --fast   只做快速检查：gofmt(协议相关文件) + go build + 单元测试 + bash -n（默认）
  --e2e    在快速检查之外，再跑一次端到端小实验并校验日志

环境变量（仅 --e2e）：
  SCHEMA          schema，默认 preempt_escrow
  SMOKE_SHARDS    分片数，默认 2
  SMOKE_NODES     每分片节点数，默认 1
  SMOKE_TXS       交易条数，默认 8000
  SMOKE_INJECT    注入速率，默认 200（慢速注入才能让付款落在迁移窗口内，
                  从而覆盖 MPTP/batch 路径；设 1000 则通常只有 lock/quiesce/settle）
  SMOKE_MCB       Max_Commit_Block，默认 2（快速触发迁移）
  SMOKE_TIMEOUT   超时秒数，默认 900
  SMOKE_FORCE     1 = 即使已有 block_exe 在跑也强制启动
EOF
}

mode="fast"
case "${1:-}" in
    ""|--fast)   mode="fast" ;;
    --e2e|--all) mode="e2e" ;;
    -h|--help)   usage; exit 0 ;;
    *)           echo "ERROR: 未知选项 '$1'（可用：--fast, --e2e, --help）" >&2; exit 2 ;;
esac

pass=0
fail=0
failures=""

ok()  { pass=$((pass + 1)); printf '  [ok]   %s\n' "$1"; }
bad() { fail=$((fail + 1)); failures="$failures
  - $1"; printf '  [FAIL] %s\n' "$1"; }
info() { printf '  [--]   %s\n' "$1"; }

echo "=== smoke_test: mode=$mode ==="

# ------------------------------------------------------------------
# 1. 格式检查（只查本仓库主动维护的协议相关文件；旧文件历史上未 gofmt）
# ------------------------------------------------------------------
echo ""
echo "-- gofmt"
fmt_files="account/escrow_state.go account/escrow_state_test.go
core/escrow.go core/escrowpool.go core/escrow_test.go core/txpool.go core/block.go core/txmig2.go
chain/blockchain.go
pbft/escrow.go pbft/pbft.go pbft/pbftsingle.go pbft/client.go pbft/cmd.go
shard/shard.go"
existing=""
for f in $fmt_files; do
    [ -f "$f" ] && existing="$existing $f"
done
if [ -z "$existing" ]; then
    bad "gofmt: 待检查文件一个都不存在（工作目录不对？）"
else
    unformatted=$(gofmt -l $existing 2>/dev/null)
    if [ -z "$unformatted" ]; then
        ok "gofmt ($(echo $existing | wc -w | tr -d ' ') files)"
    else
        bad "gofmt 未格式化: $(echo $unformatted | tr '\n' ' ')"
    fi
fi

# ------------------------------------------------------------------
# 2. 编译
# ------------------------------------------------------------------
echo ""
echo "-- build"
if go build ./... >/tmp/smoke_build.log 2>&1; then
    ok "go build ./..."
else
    bad "go build ./...（见 /tmp/smoke_build.log）"
    tail -n 20 /tmp/smoke_build.log | sed 's/^/         /'
fi

# ------------------------------------------------------------------
# 3. 单元测试
# ------------------------------------------------------------------
echo ""
echo "-- unit tests"
if go test ./account/... ./core/... ./params/... >/tmp/smoke_unit.log 2>&1; then
    ok "go test ./account/... ./core/... ./params/..."
else
    bad "go test ./account/... ./core/... ./params/...（见 /tmp/smoke_unit.log）"
    tail -n 25 /tmp/smoke_unit.log | sed 's/^/         /'
fi

if go test -vet=off ./... >/tmp/smoke_all.log 2>&1; then
    ok "go test -vet=off ./..."
else
    bad "go test -vet=off ./...（见 /tmp/smoke_all.log）"
    tail -n 25 /tmp/smoke_all.log | sed 's/^/         /'
fi

# ------------------------------------------------------------------
# 4. Shell 脚本语法
# ------------------------------------------------------------------
echo ""
echo "-- shell syntax"
sh_bad=""
for f in ./*.sh; do
    [ -f "$f" ] || continue
    bash -n "$f" 2>/dev/null || sh_bad="$sh_bad $f"
done
if [ -z "$sh_bad" ]; then
    ok "bash -n *.sh"
else
    bad "bash -n 失败:$sh_bad"
fi

# ------------------------------------------------------------------
# 5. （可选）端到端小实验
# ------------------------------------------------------------------
e2e_state="skipped"
if [ "$mode" = "e2e" ]; then
    fail_before_e2e=$fail
    can_run=1

    schema="${SCHEMA:-preempt_escrow}"
    shards="${SMOKE_SHARDS:-2}"
    nodes="${SMOKE_NODES:-1}"
    txs="${SMOKE_TXS:-8000}"
    mcb="${SMOKE_MCB:-2}"
    inject="${SMOKE_INJECT:-200}"
    timeout_s="${SMOKE_TIMEOUT:-900}"
    smoke_csv="smoke/20W.csv"
    smoke_log="/tmp/smoke_e2e.log"

    echo ""
    echo "-- e2e ($schema, ${shards} shard(s), ${nodes} node/shard, ${txs} txs, mcb=${mcb}, timeout ${timeout_s}s)"

    if [ ! -f "20W.csv" ]; then
        bad "e2e: 找不到 20W.csv"
        can_run=0
    fi
    if [ -n "$(pgrep -f '(^|/)block_exe( |$)' 2>/dev/null)" ] && [ "${SMOKE_FORCE:-0}" != "1" ]; then
        bad "e2e: 已有 block_exe 在运行，拒绝启动（先 ./kill_block_exe.sh，或 SMOKE_FORCE=1）"
        can_run=0
    fi

    if [ "$can_run" = "1" ]; then
        mkdir -p smoke
        # 数据集必须叫 20W.csv：client 按 basename 选择列布局（子目录已支持）。
        { head -n 1 20W.csv; tail -n +2 20W.csv | head -n "$txs"; } > "$smoke_csv"
        info "数据集: $smoke_csv ($(( $(wc -l < "$smoke_csv") - 1 )) txs)"

        cfg_backup=$(mktemp /tmp/smoke_config.XXXXXX)
        cp params/config.go "$cfg_backup"
        restore_cfg() { [ -f "${cfg_backup:-}" ] && cp "$cfg_backup" params/config.go; }
        trap 'restore_cfg' EXIT

        ./kill_block_exe.sh >/dev/null 2>&1

        # run.sh 只接受 SCHEMA/NUM_SHARDS/NUM_NODES，Max_Commit_Block 与 Inject_speed
        # 必须通过 set_config.sh 写入；config.go 已在上面备份，结束后会恢复。
        if ! ./set_config.sh --inject-speed "$inject" --max-commit-block "$mcb" >/dev/null; then
            bad "e2e: set_config.sh 应用 inject/mcb 失败"
            can_run=0
        fi
    fi

    if [ "$can_run" = "1" ]; then
        SCHEMA="$schema" NUM_SHARDS="$shards" NUM_NODES="$nodes" \
            TEST_FILE="$smoke_csv" NO_PACK=1 \
            ./run.sh > "$smoke_log" 2>&1 &
        run_pid=$!

        # 便携看门狗（macOS 默认没有 timeout 命令）
        ( sleep "$timeout_s"; kill -TERM "$run_pid" 2>/dev/null ) &
        watch_pid=$!

        wait "$run_pid"
        kill "$watch_pid" 2>/dev/null
        wait "$watch_pid" 2>/dev/null
        ./kill_block_exe.sh >/dev/null 2>&1

        # --- 5.1 进程日志：不能有 panic ---
        if grep -qi 'panic:' process_logs/*.log 2>/dev/null; then
            bad "e2e: 节点日志出现 panic"
            grep -hi 'panic:' process_logs/*.log 2>/dev/null | head -n 5 | sed 's/^/         /'
        else
            ok "e2e: 无 panic"
        fi

        # --- 5.2 client 正常结束 ---
        if grep -q '停止时间' process_logs/client.log 2>/dev/null; then
            ok "e2e: client 打印 停止时间"
        else
            bad "e2e: client 未打印 停止时间（可能超时；见 process_logs/client.log）"
            tail -n 15 process_logs/client.log 2>/dev/null | sed 's/^/         /'
        fi

        # --- 5.3 每个分片都出了块 ---
        missing=""
        s=0
        while [ "$s" -lt "$shards" ]; do
            [ -s "log/S${s}_block.csv" ] || missing="$missing S${s}"
            s=$((s + 1))
        done
        if [ -z "$missing" ]; then
            ok "e2e: 所有分片都有 block.csv"
        else
            bad "e2e: 缺少 block.csv:$missing"
        fi

        # --- 5.4 escrow 专项检查 ---
        if [ "$schema" = "preempt_escrow" ]; then
            if ls log/S*_escrow.csv >/dev/null 2>&1; then
                ok "e2e: 生成了 escrow.csv"
            else
                bad "e2e: 未生成 escrow.csv"
            fi

            header=$(head -n 1 log/S0_escrow_queue.csv 2>/dev/null)
            case "$header" in
                *pending_credits*) ok "e2e: escrow_queue.csv 含 pending_credits 列" ;;
                *) bad "e2e: escrow_queue.csv 缺少 pending_credits 列（header=$header）" ;;
            esac

            qbad=""
            for f in log/S*_escrow_queue.csv; do
                [ -f "$f" ] || continue
                line=$(tail -n 1 "$f")
                IFS=, read -r _q_h q_mptp q_gated q_pend q_term q_pcred <<< "$line"
                if [ "${q_mptp:-x}" != "0" ] || [ "${q_gated:-x}" != "0" ] || \
                   [ "${q_pend:-x}" != "0" ] || [ "${q_pcred:-x}" != "0" ] || \
                   [ "${q_term:-x}" != "true" ]; then
                    qbad="$qbad
         $f -> $line"
                fi
            done
            if [ -z "$qbad" ]; then
                ok "e2e: 各分片 mptp/gated/pending/pending_credits 归零且 terminal=true"
            else
                bad "e2e: 队列未排空:$qbad"
            fi

            if grep -q '\[escrow\]\[invariant\]' process_logs/*.log 2>/dev/null; then
                bad "e2e: 出现 escrow 不变量告警"
                grep -h '\[escrow\]\[invariant\]' process_logs/*.log | head -n 5 | sed 's/^/         /'
            else
                ok "e2e: 无 escrow/settlement 残留告警"
            fi

            ev=/tmp/smoke_escrow_events.csv
            cat log/S*_escrow.csv > "$ev" 2>/dev/null || : > "$ev"
            n_lock=$(awk -F, '$3=="lock"' "$ev" | wc -l | tr -d ' ')
            n_q=$(awk -F, '$3=="quiesce"' "$ev" | wc -l | tr -d ' ')
            n_settle=$(awk -F, '$3=="settle"' "$ev" | wc -l | tr -d ' ')
            n_batch=$(awk -F, '$3=="batch"' "$ev" | wc -l | tr -d ' ')
            info "事件计数: lock=$n_lock quiesce=$n_q settle=$n_settle batch=$n_batch"

            if [ "$n_lock" -gt 0 ] && [ "$n_settle" -gt 0 ]; then
                ok "e2e: 迁移链路 lock -> quiesce -> settle 已跑通"
            else
                bad "e2e: 迁移链路未跑通（lock=$n_lock settle=$n_settle）"
            fi

            if [ "$n_batch" -gt 0 ]; then
                ok "e2e: 至少授权了一个付款批次"
            else
                info "e2e: 本次没有产生 batch（负载太小或窗口内无付款，不视为失败）"
            fi

            if [ "$n_q" -gt 0 ]; then
                if awk -F, '$3=="quiesce" && $9 !~ /trigger=(delay|timeout)/' "$ev" | grep -q .; then
                    bad "e2e: quiesce 事件缺少 trigger=delay|timeout"
                else
                    ok "e2e: quiesce 事件带 trigger 标记"
                fi
            fi
        fi

        restore_cfg
        trap - EXIT
        info "e2e 运行日志: $smoke_log；结果日志保留在 ./log/ 与 ./process_logs/"
    fi

    if [ "$can_run" = "1" ] && [ "$fail" -eq "$fail_before_e2e" ]; then
        e2e_state="passed"
    else
        e2e_state="failed"
    fi
fi

# ------------------------------------------------------------------
# 汇总
# ------------------------------------------------------------------
echo ""
echo "=== smoke_test: pass=$pass fail=$fail e2e=$e2e_state ==="
if [ "$fail" -ne 0 ]; then
    echo "失败项:"
    printf '%s\n' "$failures"
    exit 1
fi
if [ "$mode" != "e2e" ]; then
    echo "提示: 需要端到端验证时运行  ./smoke_test.sh --e2e"
fi
exit 0
