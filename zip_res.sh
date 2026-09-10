#!/bin/bash

# 根据 params/config.go 中的 Schema 默认值决定输出包名：
#   preempt -> res_preempt.zip
#   caravan -> res_caravan.zip
#   ftl     -> res_finetune.zip
if grep -Eq 'Schema:[[:space:]]*SchemaPreempt' params/config.go; then
    RESULT="res_preempt"
elif grep -Eq 'Schema:[[:space:]]*SchemaCaravan' params/config.go; then
    RESULT="res_caravan"
else
    RESULT="res_finetune"
fi

echo "Schema -> ${RESULT}.zip"

rm -f "./${RESULT}.zip"
zip -r "${RESULT}.zip" log
cp "${RESULT}.zip" /disc/home/dy/caddy/site
