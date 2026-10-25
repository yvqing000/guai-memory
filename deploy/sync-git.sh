#!/usr/bin/env bash
# 归屿记忆库 -> GitHub 定期备份（cron 调用）
# 记忆真源（data/vault/**/*.md + data/config.yaml）随代码一起 commit + push。
# push 失败不丢本地提交，下一轮自动补。
set -uo pipefail
GUAI_HOME="${GUAI_HOME:-/home/ubuntu/guai-memory}"
LOG="$GUAI_HOME/sync-git.log"
cd "$GUAI_HOME" || { echo "[$(date '+%F %T')] FATAL no repo" >>"$LOG"; exit 1; }
git add -A >>"$LOG" 2>&1
if git diff --cached --quiet; then
  echo "[$(date '+%F %T')] no change" >>"$LOG"; exit 0
fi
N=$(git diff --cached --name-only | wc -l | tr -d ' ')
git -c user.name="guai-memory-bot" -c user.email="bot@guai.local" \
  commit -q -m "backup $(date '+%F %H:%M') ($N files)" >>"$LOG" 2>&1 \
  || { echo "[$(date '+%F %T')] commit failed" >>"$LOG"; exit 1; }
echo "[$(date '+%F %T')] committed $N files" >>"$LOG"
git push origin main >>"$LOG" 2>&1 \
  && echo "[$(date '+%F %T')] pushed OK" >>"$LOG" \
  || echo "[$(date '+%F %T')] push FAILED - 本地已保留, 下轮补推" >>"$LOG"
exit 0
