#!/bin/sh
set -e

CONFIG_PATH="${HLU_CONFIG:-/code/config.ini}"

if [ ! -f "$CONFIG_PATH" ] && [ -n "$HLU_CONFIG_CONTENT" ]; then
  printf '%s\n' "$HLU_CONFIG_CONTENT" > "$CONFIG_PATH"
fi

exec python /code/bot.py
