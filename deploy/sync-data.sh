#!/usr/bin/env bash
# 归屿记忆数据 -> 私密库 yvqing000/guai-memory-data
# 公开库只放代码；真实记忆（vault/*.md + config.yaml）走这里
set -uo pipefail
DATA_DIR="/home/ubuntu/guai-memory/data"
LOG="/home/ubuntu/guai-memory/sync-data.log"
log(){ echo "[$(date '+%F %T')] $*" >> "$LOG"; }
cd "$DATA_DIR" || { log "FATAL: no data dir"; exit 1; }
git add -A >>"$LOG" 2>&1
if git diff --cached --quiet; then log "no change"; exit 0; fi
N=$(git diff --cached --name-only | wc -l | tr -d ' ')
git -c user.name="guai-memory-bot" -c user.email="bot@guai.local" \
  commit -q -m "backup $(date '+%F %H:%M') ($N files)" >>"$LOG" 2>&1 \
  || { log "commit failed"; exit 1; }
log "committed $N files"
git push origin main >>"$LOG" 2>&1 \
  && log "pushed OK to guai-memory-data" \
  || log "push FAILED - 本地已保留, 下轮补推"
exit 0
