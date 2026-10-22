#!/bin/bash
#
# 批量实验：按 CSV/TSV 逐行执行一次完整的 run.sh
#   （每个实验：kill -> clean -> 改写 params/config.go -> 重编译 -> 运行 -> 结束后自动打包到 res/）
#
# 列由表头按列名识别（大小写不敏感，顺序任意；逗号或 TAB 分隔均可）：
#   scheme|schema, num_shards, Inject_speed, [num_nodes], [Max_Commit_Block], [MaxBlockSize]
#   前 3 列必填，后 3 列可选（缺省则用 config/run.sh 默认值）。
#
# 例（TAB 分隔，见 tmp.txt）：
#   scheme	num_shards	Inject_speed	num_nodes	Max_Commit_Block	MaxBlockSize
#   LB-CHAIN	4	1000	16	25	2000
#   ...
#
# 用法：
#   ./run_exp.sh [exp.csv]              # 默认 exp.csv
#   DRY_RUN=1 ./run_exp.sh tmp.txt      # 只解析并打印参数，不执行
#   TEST_FILE=20W.csv ./run_exp.sh      # 公共参数可用环境变量覆盖

set -u

csv="${1:-exp.csv}"
dry="${DRY_RUN:-0}"

if [ ! -f "$csv" ]; then
    echo "ERROR: 找不到实验清单 '$csv'（请在项目根目录运行，或传入路径）" >&2
    exit 1
fi

# 退出时清理残留节点；Ctrl-C / TERM 时终止整个批量实验（默认行为），
# 而不是只停掉当前这一行继续跑下一个实验。
cleanup_nodes() { ./kill_block_exe.sh >/dev/null 2>&1 || true; }

on_interrupt() {
    echo ""
    echo "!! 收到中断信号：终止整个批量实验（不再执行后续行）" >&2
    cleanup_nodes
    exit 130
}

trap cleanup_nodes EXIT
trap on_interrupt INT TERM

# ---------------- 解析表头：得到各列索引（0-based；未出现的列 = -1） ----------------
header_norm=$(head -n 1 "$csv" | tr '\t' ',')
idx_scheme=-1; idx_shards=-1; idx_inject=-1; idx_nodes=-1; idx_mcb=-1; idx_mbs=-1

old_ifs="$IFS"
IFS=','
i=0
for name in $header_norm; do
    name=$(printf '%s' "$name" | tr -d '[:space:]' | tr 'A-Z' 'a-z')
    case "$name" in
        scheme|schema)                    idx_scheme=$i ;;
        ns|num_shards|n_shard|shards|shard) idx_shards=$i ;;
        is|inject_speed|inject)           idx_inject=$i ;;
        nn|num_nodes|n_nodes|nodes|node)   idx_nodes=$i ;;
        mcb|max_commit_block)             idx_mcb=$i ;;
        mb|maxblocksize|max_block_size)   idx_mbs=$i ;;
    esac
    i=$((i + 1))
done
IFS="$old_ifs"

if [ "$idx_scheme" -lt 0 ] || [ "$idx_shards" -lt 0 ] || [ "$idx_inject" -lt 0 ]; then
    echo "ERROR: 表头必须包含 scheme、ns(num_shards)、is(inject_speed) 三列" >&2
    echo "       实际表头: $header_norm" >&2
    exit 1
fi

echo "=== Batch experiments from: $csv ==="
echo "    columns: scheme=$idx_scheme shards=$idx_shards inject=$idx_inject nodes=$idx_nodes mcb=$idx_mcb mbs=$idx_mbs"
[ "$dry" = "1" ] && echo "    (DRY_RUN=1：只解析，不执行)"

total=0
ok=0
fail=0

while IFS= read -r rawline || [ -n "$rawline" ]; do
    # 统一分隔符为逗号（TSV -> CSV）
    line=$(printf '%s' "$rawline" | tr '\t' ',')

    # 跳过空行 / 注释行
    stripped=$(printf '%s' "$line" | tr -d '[:space:]')
    [ -z "$stripped" ] && continue
    case "$stripped" in \#*) continue ;; esac

    IFS=',' read -ra f <<< "$line"

    scheme=""; shards=""; inject=""; nodes=""; mcb=""; mbs=""
    [ "$idx_scheme" -ge 0 ] && scheme="${f[$idx_scheme]:-}"
    [ "$idx_shards" -ge 0 ] && shards="${f[$idx_shards]:-}"
    [ "$idx_inject" -ge 0 ] && inject="${f[$idx_inject]:-}"
    [ "$idx_nodes"  -ge 0 ] && nodes="${f[$idx_nodes]:-}"
    [ "$idx_mcb"    -ge 0 ] && mcb="${f[$idx_mcb]:-}"
    [ "$idx_mbs"    -ge 0 ] && mbs="${f[$idx_mbs]:-}"

    scheme=$(printf '%s' "$scheme" | tr -d '[:space:]')
    shards=$(printf '%s' "$shards" | tr -d '[:space:]')
    inject=$(printf '%s' "$inject" | tr -d '[:space:]')
    nodes=$(printf '%s' "$nodes"  | tr -d '[:space:]')
    mcb=$(printf '%s' "$mcb"      | tr -d '[:space:]')
    mbs=$(printf '%s' "$mbs"      | tr -d '[:space:]')

    # 跳过表头（scheme 列恰好是 "scheme"/"schema" 的行）
    scheme_lc=$(printf '%s' "$scheme" | tr 'A-Z' 'a-z')
    [ "$scheme_lc" = "scheme" ] || [ "$scheme_lc" = "schema" ] && continue

    # 必填项校验
    if [ -z "$scheme" ] || [ -z "$shards" ] || [ -z "$inject" ]; then
        echo "!! 跳过无效行: '$rawline'" >&2
        continue
    fi

    total=$((total + 1))
    echo ""
    echo "############################################################"
    echo "### Experiment $total : scheme=$scheme shards=$shards inject=$inject nodes=${nodes:-<default>} Max_Commit_Block=${mcb:-<default>} MaxBlockSize=${mbs:-<default>}"
    echo "############################################################"

    if [ "$dry" = "1" ]; then
        echo "(dry-run) 不实际执行。"
        continue
    fi

    cfg_args=(--schema "$scheme" --inject-speed "$inject")
    [ -n "$mcb" ] && cfg_args+=(--max-commit-block "$mcb")
    [ -n "$mbs" ] && cfg_args+=(--max-block-size "$mbs")
    if ! ./set_config.sh "${cfg_args[@]}"; then
        echo "!! 跳过：无法应用本次配置（scheme=$scheme inject=$inject mcb=${mcb:-} mbs=${mbs:-}）" >&2
        fail=$((fail + 1)); continue
    fi

    # run.sh 读取环境变量 SCHEMA / NUM_SHARDS / NUM_NODES
    if [ -n "$nodes" ]; then
        SCHEMA="$scheme" NUM_SHARDS="$shards" NUM_NODES="$nodes" ./run.sh
    else
        SCHEMA="$scheme" NUM_SHARDS="$shards" ./run.sh
    fi
    rc=$?
    if [ "$rc" -eq 0 ]; then
        ok=$((ok + 1))
    else
        echo "!! run.sh 返回非零状态：$rc" >&2
        fail=$((fail + 1))
    fi
done < "$csv"

echo ""
echo "=== Batch finished: total=$total  ok=$ok  fail=$fail ==="
echo "Results in ./res/ :"
ls -1t res/ 2>/dev/null | head -n "$total"
