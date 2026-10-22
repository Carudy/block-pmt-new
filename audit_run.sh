#!/bin/bash
#
# audit_run.sh — 用日志核对一次运行：注入的交易都去哪了。
#
# 用法：
#   ./audit_run.sh                       # 默认读 ./log
#   ./audit_run.sh log                   # 指定日志目录
#   ./audit_run.sh res/res_xxx.zip       # 直接核对归档（自动解压到临时目录）
#
# 核心恒等式（全部来自日志，不补算）：
#   injected = committed + delegated + dropped + queued + unexplained
#
#   injected   client_inject.csv 的 count 之和（客户端交给传输层的交易）
#   committed  S*_transaction.csv 中 id>=0 的去重数量
#   delegated  S*_escrow.csv 中 batch 事件的 ntx 之和
#   dropped    S*_intake.csv 末行：dropped_not_own + dropped_dup + dropped_no_nonce + gap_repair
#   queued     S*_queue_dump.csv 的数据行数（运行结束时仍留在队列里的交易）
#
# 退出码：0 = unexplained 为 0（或缺少台账无法判定）；1 = 存在无法解释的交易。

set -u

src="${1:-log}"
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
    echo "用法: ./audit_run.sh [log_dir|result.zip]" >&2
    exit 2
fi

num() { printf '%s' "${1:-0}" | tr -cd '0-9'; }

# ---------- 注入 ----------
injected="NA"
if compgen -G "$src/client_inject.csv" >/dev/null 2>&1; then
    injected=$(awk -F, 'FNR>1 {s+=$3} END {printf "%d", s+0}' "$src"/client_inject.csv)
    inj_lo=$(awk -F, 'FNR>1 {if (!seen || $4+0<lo) lo=$4+0; seen=1} END {print lo+0}' "$src"/client_inject.csv)
    inj_hi=$(awk -F, 'FNR>1 {if ($5+0>hi) hi=$5+0} END {print hi+0}' "$src"/client_inject.csv)
fi

# ---------- 提交 ----------
committed="NA"
if compgen -G "$src/S*_transaction.csv" >/dev/null 2>&1; then
    committed=$(awk -F, 'FNR>1 && $3+0>=0 {print $3+0}' "$src"/S*_transaction.csv | sort -u | wc -l | tr -d ' ')
fi

# ---------- 委托 ----------
delegated=0
if compgen -G "$src/S*_escrow.csv" >/dev/null 2>&1; then
    delegated=$(awk -F, '
        FNR>1 && $3=="batch" {
            n=split($9, a, " ")
            for (i=1;i<=n;i++) if (a[i] ~ /^ntx=/) { split(a[i], b, "="); s += b[2] }
        } END { printf "%d", s+0 }' "$src"/S*_escrow.csv)
fi

# ---------- 丢弃 / 残留 ----------
dropped="NA"
dropped_detail=""
if compgen -G "$src/S*_intake.csv" >/dev/null 2>&1; then
    dropped=0
    d_notown=0; d_dup=0; d_nononce=0; d_gap=0; d_relay=0
    for f in "$src"/S*_intake.csv; do
        [ -f "$f" ] || continue
        line=$(tail -n 1 "$f")
        read -r _h _rec _fwd a b c _g _m d e <<< "$(printf '%s' "$line" | tr ',' ' ')"
        a=$(num "$a"); b=$(num "$b"); c=$(num "$c"); d=$(num "$d"); e=$(num "$e")
        d_notown=$((d_notown + a))
        d_dup=$((d_dup + b))
        d_nononce=$((d_nononce + c))
        d_gap=$((d_gap + d))
        d_relay=$((d_relay + e))
    done
    dropped=$((d_notown + d_dup + d_nononce + d_gap + d_relay))
    dropped_detail="dropped_not_own=$d_notown dropped_relay=$d_relay dropped_dup=$d_dup dropped_no_nonce=$d_nononce gap_repair=$d_gap"
fi

queued="NA"
queued_user=0
queued_relay=0
if compgen -G "$src/S*_queue_dump.csv" >/dev/null 2>&1; then
    queued=0
    for f in "$src"/S*_queue_dump.csv; do
        [ -s "$f" ] || continue
        n=$(( $(wc -l < "$f") - 1 ))
        [ "$n" -gt 0 ] && queued=$((queued + n))
    done
    # 对账只算用户交易（txid >= 0）；负数 id 是交付阶段生成的合成 relay 交易。
    queued_user=$(awk -F, 'FNR>1 && $2+0>=0' "$src"/S*_queue_dump.csv | wc -l | tr -d ' ')
    queued_relay=$((queued - queued_user))
fi

# ---------- 汇总 ----------
echo "=== audit_run: $src ==="
echo ""
if [ "$injected" != "NA" ]; then
    echo "injected            $injected   (txid $inj_lo..$inj_hi)"
else
    echo "injected            NA         (缺少 client_inject.csv，无法对账)"
fi
[ "$committed" != "NA" ] && echo "committed           $committed   (S*_transaction.csv 去重 id>=0)"
[ -n "$dropped_detail" ] && echo "dropped             $dropped   ($dropped_detail)"
[ "$queued" != "NA" ] && echo "queued at stop      $queued   (用户交易 $queued_user, 合成 relay $queued_relay)"
if [ "$delegated" -ne 0 ]; then
    echo "delegated           $delegated   (batch ntx 之和)"
fi

rc=0
if [ "$injected" != "NA" ] && [ "$committed" != "NA" ] && [ "$dropped" != "NA" ] && [ "$queued" != "NA" ]; then
    unexplained=$((injected - committed - delegated - dropped - queued_user))
    echo "---------------------------------------------"
    echo "unexplained         $unexplained"
    if [ "$unexplained" -ne 0 ]; then
        echo ""
        echo "!! 对账不平：$unexplained 笔交易既没有提交、没有委托、没有记录丢弃、也不在队列里。"
        echo "   优先检查 S*_intake.csv 的 received/forwarded_out 与客户端注入是否一致。"
        rc=1
    fi
    if [ "$queued_user" -ne 0 ]; then
        echo "   注意：$queued_user 笔用户交易停机时仍在队列里（会被算作已对账，但没有执行）。"
    fi
fi

# ---------- escrow 指标 ----------
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

# ---------- 不变量告警（有 process_logs 时） ----------
logdir="$(dirname "$src")/process_logs"
if [ -d "$logdir" ] && compgen -G "$logdir/*.log" >/dev/null 2>&1; then
    n_inv=$(grep -h '\[escrow\]\[invariant\]' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    n_gap=$(grep -h '\[escrow\]\[gap-repair\]' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    n_panic=$(grep -hi 'panic:' "$logdir"/*.log 2>/dev/null | wc -l | tr -d ' ')
    echo ""
    echo "--- process_logs ---"
    echo "invariant_warnings=$n_inv gap_repair_events=$n_gap panics=$n_panic"
    [ "$n_inv" -ne 0 ] && rc=1
fi

exit "$rc"
