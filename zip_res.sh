#!/bin/bash

# 打包实验结果到 res/ 目录，命名格式： res_{schema}_n{shards}_s{inject}.zip
#   例：res_caravan_n2_s500.zip
#
# schema / inject_speed 从 params/config.go 的【生效行】读取；
# 分片数默认从 run.sh 的 num_shards 读取，也可用环境变量覆盖：
#   N_SHARD=2 ./zip_res.sh

source ./schema_detect.sh
schema_detect || exit 1

if [ -z "${N_SHARD}" ]; then
    N_SHARD=$(sed -n 's/^[[:space:]]*num_shards=\([0-9][0-9]*\).*/\1/p' run.sh | head -n 1)
fi
if [ -z "${N_SHARD}" ]; then
    echo "ERROR: 无法确定分片数（run.sh 中未找到 num_shards；可用 N_SHARD=2 ./zip_res.sh 覆盖）" >&2
    exit 1
fi

STAMP=$(date +%Y%m%d%H%M)
NAME="res_${SCHEMA}_n${N_SHARD}_s${INJECT_SPEED}_${STAMP}"
OUT="res/${NAME}.zip"

mkdir -p res
rm -f "${OUT}"
zip -r "${OUT}" log
cp "${OUT}" /disc/home/dy/caddy/site

echo "Wrote ${OUT}  (schema=${SCHEMA}, shards=${N_SHARD}, inject_speed=${INJECT_SPEED})"
