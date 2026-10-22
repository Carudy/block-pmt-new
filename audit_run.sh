#!/bin/bash
#
# audit_run.sh — 用日志核对一次运行：数据集里的每一笔交易都去哪了。
#
# 用法：
#   ./audit_run.sh                        # 默认读 ./log 与 20W.csv
#   DATASET=smoke/20W.csv ./audit_run.sh log
#   DATASET=20W.csv ./audit_run.sh res/res_xxx.zip
#
# 口径（D4）：
#   分母来自数据集文件本身：总行数、以及客户端过滤后真正会注入的行数。
#   每笔交易必须且只能落入以下四类之一（按优先级去重）：
#     1. committed  普通上链（S*_transaction.csv，txid >= 0）
#     2. delegated  委托（S*_escrow_tx.csv 里有 auth_height 的 txid，未上链）
#     3. dropped    丢弃（S*_escrow_drop.csv）
#     4. queued     运行结束时仍在队列（S*_queue_dump.csv，txid >= 0）
#   四类互斥，并集应等于数据集里的交易集合。
#
# 退出码：0 = 并集覆盖数据集且没有无法解释的交易；1 = 不平。

set -u

src="${1:-log}"
dataset="${DATASET:-20W.csv}"
tmpdir=""

cleanup() { [ -n "$tmpdir" ] && rm -rf "$tmpdir"; }
trap cleanup EXIT

case "$src" in
    *.zip)
        command -v unzip >/dev/null 2>&1 || { echo "ERROR: 需要 unzip" >&2; exit 2; }
        tmpdir=$(mktemp -d /tmp/audit_run.XXXXXX)
        unzip -q -o "$src" -d "$tmpdir" || { echo "ERROR: 解压失败: $src" >&2; exit 2; }
        src="$tmpdir/log"
        ;;
esac

if [ ! -d "$src" ]; then
    echo "ERROR: 找不到日志目录 '$src'" >&2
    echo "用法: [DATASET=20W.csv] ./audit_run.sh [log_dir|result.zip]" >&2
    exit 2
fi

num() { printf '%s' "${1:-0}" | tr -cd '0-9'; }

# ------------------------------------------------------------------
# 数据集口径：总行数 + 客户端过滤后注入的交易数
# 过滤规则与 pbft/client.go Get_Initial_Map_And_TXS 保持一致：
#   跳过 合约创建(row6 != None)、row7/row8 标记、地址非 40 hex、自转账。
# ------------------------------------------------------------------
ds_rows="NA"; ds_txs="NA"
if [ -f "$dataset" ]; then
    ds_stats=$(awk -F, '
        NR==1 {next}
        { rows++
          if ($6 != "None" || $7 == "1" || $8 == "1" ||
              length(substr($5,3)) != 40 || length(substr($4,3)) != 40 || $5 == $4) next
          kept++ }
        END { printf "%d %d", rows+0, kept+0 }' "$dataset")
    ds_rows=${ds_stats%% *}
    ds_txs=${ds_stats##* }
else
    echo "WARN: 找不到数据集 '$dataset'，无法给出 D4 口径的分母（可用 DATASET=... 指定）" >&2
fi

# ------------------------------------------------------------------
# 四类集合
# ------------------------------------------------------------------
work=$(mktemp -d /tmp/audit_sets.XXXXXX)
# 复用 cleanup：把工作目录一起删掉
cleanup() { rm -rf "$work"; [ -n "$tmpdir" ] && rm -rf "$tmpdir"; }
trap cleanup EXIT

: > "$work/committed"
: > "$work/delegated"
: > "$work/dropped"
: > "$work/queued"

if compgen -G "$src/S*_transaction.csv" >/dev/null 2>&1; then
    awk -F, 'FNR>1 && $3+0>=0 {print $3+0}' "$src"/S*_transaction.csv | sort -u > "$work/committed"
fi
if compgen -G "$src/S*_escrow_tx.csv" >/dev/null 2>&1; then
    awk -F, 'FNR>1 && $9!="" {print $1+0}' "$src"/S*_escrow_tx.csv | sort -u > "$work/delegated"
fi
if compgen -G "$src/S*_escrow_drop.csv" >/dev/null 2>&1; then
    awk -F, 'FNR>1 && $1+0>=0 {print $1+0}' "$src"/S*_escrow_drop.csv | sort -u > "$work/dropped"
fi
if compgen -G "$src/S*_queue_dump.csv" >/dev/null 2>&1; then
    awk -F, 'FNR>1 && $2+0>=0 {print $2+0}' "$src"/S*_queue_dump.csv | sort -u > "$work/queued"
fi

n_committed=$(wc -l < "$work/committed" | tr -d ' ')
n_delegated_raw=$(wc -l < "$work/delegated" | tr -d ' ')
n_dropped_raw=$(wc -l < "$work/dropped" | tr -d ' ')
n_queued_raw=$(wc -l < "$work/queued" | tr -d ' ')

# 互斥划分：committed > delegated > dropped > queued
comm -13 "$work/committed" "$work/delegated" > "$work/delegated_only"
sort -u "$work/committed" "$work/delegated_only" > "$work/covered1"
comm -13 "$work/covered1" "$work/dropped" > "$work/dropped_only"
sort -u "$work/covered1" "$work/dropped_only" > "$work/covered2"
comm -13 "$work/covered2" "$work/queued" > "$work/queued_only"

n_delegated=$(wc -l < "$work/delegated_only" | tr -d ' ')
n_dropped=$(wc -l < "$work/dropped_only" | tr -d ' ')
n_queued=$(wc -l < "$work/queued_only" | tr -d ' ')

sort -u "$work/covered2" "$work/queued_only" > "$work/union"
n_union=$(wc -l < "$work/union" | tr -d ' ')

# 重叠（同一 txid 出现在多个集合，例如被委托后又重放上链）
n_replay=$(comm -12 "$work/committed" "$work/delegated" | wc -l | tr -d ' ')

echo "=== audit_run: $src ==="
echo ""
[ "$ds_rows" != "NA" ] && echo "dataset rows        $ds_rows   ($dataset)"
[ "$ds_txs"  != "NA" ] && echo "dataset txs         $ds_txs   (客户端过滤后会被注入的笔数)"
echo ""
echo "committed           $n_committed   (普通上链)"
echo "delegated           $n_delegated   (委托，escrow_tx.csv 的 auth 行；原始 $n_delegated_raw)"
echo "dropped             $n_dropped   (escrow_drop.csv；原始 $n_dropped_raw)"
echo "queued at stop      $n_queued   (运行结束时仍在队列；原始 $n_queued_raw)"
echo "---------------------------------------------"
echo "union               $n_union"

rc=0
if [ "$ds_txs" != "NA" ]; then
    unexplained=$((ds_txs - n_union))
    echo "unexplained         $unexplained"
    if [ "$unexplained" -ne 0 ]; then
        echo ""
        echo "!! 四类集合的并集没有覆盖数据集（差 $unexplained 笔）。"
        echo "   检查 S*_intake.csv 的 received / forwarded_out，以及 client_inject.csv。"
        rc=1
    fi
fi
if [ "$n_replay" -ne 0 ]; then
    echo ""
    echo "注意：$n_replay 笔 txid 既出现在 delegated 又出现在 committed（被委托后重放上链），"
    echo "      已按 committed 归类；这些是未被记录的批次，应同时出现在 drop 的 gap/退款链路上。"
fi

# ------------------------------------------------------------------
# 跨分片块高不可比，延迟请用 *_time 列
# ------------------------------------------------------------------
if compgen -G "$src/S*_escrow_tx.csv" >/dev/null 2>&1; then
    awk -F, 'FNR>1 && $9!=""{printf "%s\n", $1}' "$src"/S*_escrow_tx.csv | sort -u > "$work/auth_ids"
    awk -F, 'FNR>1 && $11!=""{printf "%s\n", $1}' "$src"/S*_escrow_tx.csv | sort -u > "$work/rec_ids"
    awk -F, 'FNR>1 && $14!=""{printf "%s\n", $1}' "$src"/S*_escrow_tx.csv | sort -u > "$work/dlv_ids"
    n_rec_missing=$(comm -23 "$work/auth_ids" "$work/rec_ids" | wc -l | tr -d ' ')
    n_dlv_missing=$(comm -23 "$work/auth_ids" "$work/dlv_ids" | wc -l | tr -d ' ')
    echo ""
    echo "auth 无 record 记录   $n_rec_missing   (batch relay 未到达目的分片)"
    echo "auth 无 deliver 记录  $n_dlv_missing   (结算后未送达收款方)"
fi

# ------------------------------------------------------------------
# escrow 指标
# ------------------------------------------------------------------
if compgen -G "$src/S*_escrow.csv" >/dev/null 2>&1; then
    echo ""
    echo "--- escrow ---"
    awk -F, '
        FNR>1 && $3=="lock"    {lock++}
        FNR>1 && $3=="quiesce" {q++; if ($9=="S*=0") s0++}
        FNR>1 && $3=="settle"  {settle++}
        FNR>1 && $3=="debit"   {debit++}
        FNR>1 && $3=="batch"   {batch++}
        END {
            printf "lock=%d quiesce=%d (S*=0: %d) settle=%d debit=%d batch=%d\n", lock, q, s0, settle, debit, batch
        }' "$src"/S*_escrow.csv

    if compgen -G "$src/S*_escrow_queue.csv" >/dev/null 2>&1; then
        echo "shard  max_mptp  max_gated  drain_h  last(pending,credits)  terminal"
        for f in "$src"/S*_escrow_queue.csv; do
            [ -f "$f" ] || continue
            shard=$(basename "$f" | cut -d_ -f1)
            awk -F, -v s="$shard" '
                FNR>1 {
                    if ($2+0>m1) m1=$2+0
                    if ($3+0>m2) m2=$3+0
                    if ($3+0>0) dr=$1
                    pend=$4; cred=$6; t=$5
                }
                END { printf "%-5s  %8d  %9d  %7d  %20s  %s\n", s, m1, m2, dr, "(" pend "," cred ")", t }' "$f"
        done
    fi
fi

# ------------------------------------------------------------------
# 丢弃原因分布 + 不变量告警
# ------------------------------------------------------------------
if compgen -G "$src/S*_escrow_drop.csv" >/dev/null 2>&1; then
    echo ""
    echo "--- drop reasons ---"
    awk -F, 'FNR>1 {c[$5]++} END {for (k in c) printf "%s=%d ", k, c[k]; print ""}' "$src"/S*_escrow_drop.csv
fi

logdir="$(dirname "$src")/process_logs"
if [ -d "$logdir" ] && compgen -G "$logdir/*.log" >/dev/null 2>&1; then
    n_inv=$(grep -h '\[escrow\]\[invariant\]' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    n_gap=$(grep -h '\[escrow\]\[gap\]' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    n_panic=$(grep -hi 'panic:' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    echo ""
    echo "--- process_logs ---"
    echo "invariant_warnings=$n_inv gap_events=$n_gap panics=$n_panic"
    [ "$n_inv" -ne 0 ] && rc=1
    [ "$n_panic" -ne 0 ] && rc=1
fi

exit "$rc"
