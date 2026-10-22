#!/bin/bash
#
# 把 res/ 目录下所有实验结果 zip 重新打包成一个总包，便于一次性下载。
# 输出： ./res_all_<YYYYMMDDHHMM>.zip   （并复制到 caddy 站点目录）
#
# 用法： ./res_all.sh            # 打包 res/ 下全部 *.zip
#        ./res_all.sh out.zip    # 指定输出文件名（仍会复制到 caddy）

set -e

out="${1:-}"
if [ -z "$out" ]; then
    out="res_all_$(date +%Y%m%d%H%M).zip"
fi

if [ ! -d res ]; then
    echo "ERROR: 找不到 res/ 目录（先跑实验生成结果）" >&2
    exit 1
fi

# 输出文件不能放在 res/ 内，否则会递归打包自己
case "$out" in
    res/*) echo "ERROR: 输出文件不要放在 res/ 内（会递归打包），请放在项目根目录" >&2; exit 1 ;;
esac

# 统计待打包的 zip 数量
count=0
for f in res/*.zip; do
    [ -e "$f" ] || continue
    count=$((count + 1))
done

if [ "$count" -eq 0 ]; then
    echo "ERROR: res/ 下没有可打包的 .zip 文件" >&2
    exit 1
fi

echo "Bundling $count result zip(s) from res/ -> $out"
rm -f "$out"
zip -j "$out" res/*.zip

cp "$out" /disc/home/dy/caddy/site

echo "Wrote ${out}  (bundled ${count} result zip(s))"
