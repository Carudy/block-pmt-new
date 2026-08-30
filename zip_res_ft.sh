#!/bin/bash

# 根据 params/config.go 中的 EnableBankMechanism 决定输出包名：
#   true  -> res_preempt.zip
#   false -> res_finetune.zip
if grep -Eq 'EnableBankMechanism:[[:space:]]*true' params/config.go; then
    RESULT="res_preempt"
else
    RESULT="res_finetune"
fi

echo "EnableBankMechanism -> ${RESULT}.zip"

rm -f "./${RESULT}.zip"
zip -r "${RESULT}.zip" log
cp "${RESULT}.zip" /disc/home/dy/caddy/site
