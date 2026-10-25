#!/usr/bin/env python3
"""给所有已有记忆补向量（首次启用语义检索时跑一次）。"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server as S

conn = S.db()
rows = conn.execute("SELECT id, title, content FROM buckets WHERE superseded_by=''").fetchall()
conn.close()
ok = 0
for r in rows:
    if S.store_embedding(r["id"], (r["title"] or "") + "\n" + (r["content"] or "")):
        ok += 1
        print("ok", r["id"])
print(f"done {ok}/{len(rows)}")
