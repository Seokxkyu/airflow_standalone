#!/bin/bash

DONE_PATH=$1

echo "check"

# 파일 존재 여부 확인
if [[ -e "$DONE_PATH" ]]; then
    figlet "Let's move on"
    exit 0
else
    echo "I'll be back => $DONE_PATH_FILE"
    exit 1
fi
