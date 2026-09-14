#!/bin/bash
# 将 params/config.go 中【生效的】Schema 行设置为指定方案，其余方案的 Schema 行注释掉。
# 用法： ./set_schema.sh <lb|ftl|caravan|preempt>

set -e

name="$1"
case "$name" in
    lb|LB)           target="SchemaLB" ;;
    ftl|FTL)         target="SchemaFTL" ;;
    caravan|Caravan) target="SchemaCaravan" ;;
    preempt|Preempt) target="SchemaPreempt" ;;
    *) echo "usage: $0 <lb|ftl|caravan|preempt>" >&2; exit 1 ;;
esac

file="params/config.go"
tmp="$file.tmp"

if [ ! -f "$file" ]; then
    echo "ERROR: 找不到 $file（请在项目根目录运行）" >&2
    exit 1
fi

# 把每一行形如  [//] Schema:  SchemaXXX,  的行规范化：目标方案取消注释，其余注释掉。
perl -e '
    my ($target) = @ARGV;
    while (my $line = <STDIN>) {
        if ($line =~ /^(\s*)(?:\/\/\s*)?Schema:\s*(SchemaLB|SchemaFTL|SchemaCaravan|SchemaPreempt)\b/) {
            my ($indent, $name) = ($1, $2);
            if ($name eq $target) {
                $line = $indent . "Schema:             " . $name . ",\n";
            } else {
                $line = $indent . "// Schema:             " . $name . ",\n";
            }
        }
        print $line;
    }
' "$target" < "$file" > "$tmp"

mv "$tmp" "$file"

# 校验目标方案确实已被激活（避免 config 里缺少该方案的备选行时静默失败）
if ! grep -Eq "^[[:space:]]*Schema:[[:space:]]*$target," "$file"; then
    echo "ERROR: 未能在 $file 中激活 $target（请确认该方案的 // Schema 备选行存在）" >&2
    exit 1
fi

echo "params/config.go: Schema -> $target"
