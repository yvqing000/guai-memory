#!/usr/bin/env bash
# 每晚：记忆整理（跳过 pinned）+ 情绪结算 + 生成梦 + 补齐缺失向量
# 面板已免密；若 .env 里设了密码则带上，否则直连。
set -uo pipefail
GUAI_HOME="${GUAI_HOME:-/home/ubuntu/guai-memory}"
LOG="$GUAI_HOME/dream.log"
PW=$(grep '^GUAI_DASHBOARD_PASSWORD=' "$GUAI_HOME/.env" | cut -d= -f2- || true)
BODY='{"dry":false,"window_hours":24}'
if [ -n "${PW:-}" ]; then
  RESP=$(curl -s --max-time 180 -X POST "http://127.0.0.1:18002/api/dream" \
    -H "Authorization: Bearer $PW" -H "Content-Type: application/json" -d "$BODY")
else
  RESP=$(curl -s --max-time 180 -X POST "http://127.0.0.1:18002/api/dream" \
    -H "Content-Type: application/json" -d "$BODY")
fi
echo "[$(date '+%F %T')] $RESP" >> "$LOG"
