#!/bin/bash
#
# 统一修改 params/config.go 中的实验配置（取代原来的 set_schema.sh / set_num_param.sh / set_inject_speed.sh）。
# 可一次设置多项；仅改动 params/config.go，其余同名行保持/置为注释；数值字段只替换数字，保持 gofmt 对齐。
#
# 用法：
#   ./set_config.sh --schema <name> [--inject-speed <n>] [--max-commit-block <n>] [--max-block-size <n>]
#
# Schema 别名（大小写、连字符/下划线/空格均忽略）：
#   lb / LB / LB-CHAIN / lb_chain / lbchain      -> SchemaLB
#   ftl / FTL                                    -> SchemaFTL
#   caravan / CARAVAN                            -> SchemaCaravan
#   preempt / PREEMPT                            -> SchemaPreempt
#
# 例：
#   ./set_config.sh --schema lb-chain
#   ./set_config.sh --inject-speed 1000 --max-commit-block 25 --max-block-size 2000

set -u

cfg="params/config.go"

usage() {
    cat >&2 <<'EOF'
用法: ./set_config.sh [选项...]
  --schema <name>          设置方案（别名: lb|lb-chain|LB-CHAIN|ftl|caravan|preempt ...）
  --is  <n>                设置 Inject_speed（= --inject-speed）
  --mcb <n>                设置 Max_Commit_Block（= --max-commit-block）
  --mb  <n>                设置 MaxBlockSize（= --max-block-size）
可一次指定多个；只修改 params/config.go。
EOF
}

if [ ! -f "$cfg" ]; then
    echo "ERROR: 找不到 $cfg（请在项目根目录运行）" >&2
    exit 1
fi

# 别名 -> Go 常量名；成功打印常量名并返回 0，否则返回 1
resolve_schema_const() {
    local s
    s=$(printf '%s' "$1" | tr 'A-Z' 'a-z' | tr -cd '[:alnum:]')
    case "$s" in
        lb|lbchain)      echo "SchemaLB" ;;
        ftl)             echo "SchemaFTL" ;;
        caravan)         echo "SchemaCaravan" ;;
        preempt)         echo "SchemaPreempt" ;;
        preemptescrow)   echo "SchemaPreemptEscrow" ;;
        *) return 1 ;;
    esac
}

set_schema_val() {
    local target tmp
    target=$(resolve_schema_const "$1") || { echo "ERROR: 未知方案 '$1'" >&2; return 1; }
    tmp="$cfg.tmp"
    perl -e '
        my ($target) = @ARGV;
        while (my $line = <STDIN>) {
            if ($line =~ /^(\s*)(?:\/\/\s*)?Schema:\s*(SchemaLB|SchemaFTL|SchemaCaravan|SchemaPreempt|SchemaPreemptEscrow)\b/) {
                my ($indent, $name) = ($1, $2);
                if ($name eq $target) { $line = $indent . "Schema: " . $name . ",\n"; }
                else                  { $line = $indent . "// Schema: " . $name . ",\n"; }
            }
            print $line;
        }
    ' "$target" < "$cfg" > "$tmp" || { rm -f "$tmp"; return 1; }
    mv "$tmp" "$cfg"
    if ! grep -Eq "^[[:space:]]*Schema:[[:space:]]*$target," "$cfg"; then
        echo "ERROR: 未能在 $cfg 中激活 $target（缺少该方案的 // Schema 备选行？）" >&2
        return 1
    fi
    echo "params/config.go: Schema -> $target"
}

set_num_val() {
    local key="$1" val="$2" tmp rc
    case "$val" in
        ''|*[!0-9]*) echo "ERROR: $key 必须是非负整数: '$val'" >&2; return 1 ;;
    esac
    tmp="$cfg.tmp"
    rc=0
    perl -e '
        my ($key, $target) = @ARGV;
        my $first = 1;
        my $found = 0;
        while (my $line = <STDIN>) {
            if ($line =~ /^\s*(?:\/\/\s*)?\Q$key\E:\s*[0-9]+/) {
                $found++;
                if ($first) {
                    $line =~ s/^(\s*)(?:\/\/\s*)?(\Q$key\E:\s*)[0-9]+/$1$2$target/;
                    $first = 0;
                } else {
                    unless ($line =~ /^\s*\/\//) { $line =~ s/^(\s*)/$1\/\/ /; }
                }
            }
            print $line;
        }
        exit($found ? 0 : 3);
    ' "$key" "$val" < "$cfg" > "$tmp" || rc=$?
    if [ "$rc" -ne 0 ]; then
        rm -f "$tmp"
        echo "ERROR: 在 $cfg 中找不到字段 '$key'" >&2
        return 1
    fi
    mv "$tmp" "$cfg"
    if ! grep -Eq "^[[:space:]]*$key:[[:space:]]*$val," "$cfg"; then
        echo "ERROR: 未能激活 $key=$val" >&2
        return 1
    fi
    echo "params/config.go: $key -> $val"
}

# 布尔字段：只替换生效行的 true/false
set_bool_val() {
    local key="$1" val="$2" tmp
    tmp="$cfg.tmp"
    perl -e '
        my ($key, $target) = @ARGV;
        while (my $line = <STDIN>) {
            if ($line =~ /^(\s*)(?:\/\/\s*)?(\Q$key\E:\s*)(true|false)/) {
                $line = $1 . $2 . $target . ",\n";
            }
            print $line;
        }
    ' "$key" "$val" < "$cfg" > "$tmp" || { rm -f "$tmp"; return 1; }
    mv "$tmp" "$cfg"
    if ! grep -Eq "^[[:space:]]*$key:[[:space:]]*$val," "$cfg"; then
        echo "ERROR: 未能设置 $key=$val（缺少该字段？）" >&2
        return 1
    fi
    echo "params/config.go: $key -> $val"
}

status=0
while [ $# -gt 0 ]; do
    opt="$1"; shift
    case "$opt" in
        --schema|--scheme)
            [ $# -ge 1 ] || { echo "ERROR: $opt 缺少参数" >&2; exit 1; }
            set_schema_val "$1" || status=1
            shift ;;
        --inject-speed|--injectspeed|--inject|--is)
            [ $# -ge 1 ] || { echo "ERROR: $opt 缺少参数" >&2; exit 1; }
            set_num_val Inject_speed "$1" || status=1
            shift ;;
        --max-commit-block|--mcb)
            [ $# -ge 1 ] || { echo "ERROR: $opt 缺少参数" >&2; exit 1; }
            set_num_val Max_Commit_Block "$1" || status=1
            shift ;;
        --max-block-size|--maxblocksize|--mb)
            [ $# -ge 1 ] || { echo "ERROR: $opt 缺少参数" >&2; exit 1; }
            set_num_val MaxBlockSize "$1" || status=1
            shift ;;
        --allow-confirmed-loss)
            set_bool_val EscrowSkipConfirmedLost true || status=1 ;;
        --strict-loss)
            set_bool_val EscrowSkipConfirmedLost false || status=1 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "ERROR: 未知选项 '$opt'" >&2
            usage; exit 1 ;;
    esac
done

exit "$status"
