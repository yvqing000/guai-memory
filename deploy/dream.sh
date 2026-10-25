#!/usr/bin/env bash
# 每晚：记忆整理（跳过 pinned）+ 情绪结算 + 生成梦
# 由 cron 在备份完成之后调用
set -uo pipefail
GUAI_HOME="${GUAI_HOME:-/home/ubuntu/guai-memory}"
LOG="$GUAI_HOME/dream.log"
PW=$(grep '^GUAI_DASHBOARD_PASSWORD=' "$GUAI_HOME/.env" | cut -d= -f2-)
if [ -z "$PW" ]; then echo "[$(date '+%F %T')] FATAL: no password" >> "$LOG"; exit 1; fi
RESP=$(curl -s --max-time 120 -X POST "http://127.0.0.1:18002/api/dream" \
  -H "Authorization: Bearer $PW" -H "Content-Type: application/json" \
  -d '{"dry":false,"window_hours":24}')
echo "[$(date '+%F %T')] $RESP" >> "$LOG"
