#!/bin/bash
# 供其他脚本 source 使用：从 params/config.go 读取【生效】的 Schema 与 Inject_speed。
#
# 用法：
#   source ./schema_detect.sh
#   schema_detect || exit 1     # 成功后得到 $SCHEMA 与 $INJECT_SPEED
#
# 只匹配未被注释的行（^ 允许前导空白；注释行以 // 开头，不会被匹配）。

schema_detect() {
    if   grep -Eq '^[[:space:]]*Schema:[[:space:]]*SchemaLB'      params/config.go; then SCHEMA="lb"
    elif grep -Eq '^[[:space:]]*Schema:[[:space:]]*SchemaFTL'     params/config.go; then SCHEMA="ftl"
    elif grep -Eq '^[[:space:]]*Schema:[[:space:]]*SchemaCaravan' params/config.go; then SCHEMA="caravan"
    elif grep -Eq '^[[:space:]]*Schema:[[:space:]]*SchemaPreempt' params/config.go; then SCHEMA="preempt"
    else
        echo "ERROR: params/config.go 中没有生效（未注释）的 Schema 行" >&2
        return 1
    fi

    INJECT_SPEED=$(sed -n 's/^[[:space:]]*Inject_speed:[[:space:]]*\([0-9][0-9]*\).*/\1/p' params/config.go | head -n 1)
    if [ -z "$INJECT_SPEED" ]; then
        echo "ERROR: params/config.go 中没有生效的 Inject_speed" >&2
        return 1
    fi
    return 0
}
