#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
归屿记忆系统 (Guai Memory) · 服务端 v6.2 实现

设计依据：docs/DESIGN_V6.md
存储：SQLite（结构 + FTS5 trigram 检索） + Markdown vault（真源，Obsidian 兼容）
接口：/mcp（JSON-RPC 2.0，给归屿 App 用） + /api/*（Dashboard） + /（可视化面板）

五条不可违反的不变量
  1. pinned=1 的记录只有 actor=user 能改/删；AI 对它只能 ADD / NOOP
  2. 自动累积 importance 封顶 80；81–99 只能人工升并留审计
  3. 检索命中不加分（搜到不代表重要）
  4. 记忆只淡出不删除（DELETE = 标 superseded_by）
  5. 本地/云端二选一，永不双写

情绪两档（v6.2，对应鱼仔 10-25 的最终规则）：
  - emotion_capture (version=0)：第一次记住它时的心情，**永不覆盖**
  - emotion_last    (version=N, N>=1)：之后每次回忆都可以生成新标签并自由覆盖；
    version 只作「第几次」的计数，不做保护。被覆盖的旧 last 压进 emotion_history（≤5）。
  “第二次生成且第一次仍保留” 由 capture 永不覆盖天然满足，无需单独保护 v1。
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import struct
import threading
import time
import urllib.error
import urllib.request
import uuid
import random
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import Body, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

# ─────────────────────────── 配置 ───────────────────────────

BASE = Path(os.environ.get("GUAI_HOME", Path(__file__).resolve().parent))
DATA = Path(os.environ.get("GUAI_DATA", BASE / "data"))
VAULT = Path(os.environ.get("GUAI_VAULT", DATA / "vault"))
DB_PATH = Path(os.environ.get("GUAI_DB", DATA / "guai.db"))
CONFIG_PATH = Path(os.environ.get("GUAI_CONFIG", DATA / "config.yaml"))
WEB_DIR = BASE / "web"

for d in (DATA, VAULT):
    d.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIG: dict[str, Any] = {
    "app_name": "归屿记忆",
    "owner": "鱼仔",
    "ai_name": "宴",
    "initial": {"persona": 100, "must": 100, "major": 80, "life": 50, "work": 30},
    "auto_cap": 80,
    "no_decay_days": 90,
    "decay_high": 0.95,
    "decay_low": 0.90,
    "protect_days": [180, 120, 60, 0],
    "inject": {
        "l0_tokens": 600,
        "l1_tokens": 1200,
        "l2_pick": 8,
        "l2_quota": {"79-80": 2, "70-78": 3, "60-69": 2, "50-59": 1},
        "same_domain_max": 3,
        "recent_penalty": 0.5,
        "recent_window": 3,
        "max_item_chars": 300,
    },
    "storage": "local",
    "mirror_enabled": True,
    # 自动打标（LLM）：写完一条后自动判断领域/标签/类型
    "auto_tag": True,
    "dream_keep": 100,          # 梦记忆只保留最近 N 条，超出的删最早的
    "llm": {"api_key": os.environ.get("GUAI_LLM_API_KEY", ""),
            "base_url": os.environ.get("GUAI_LLM_BASE_URL", ""),
            "model": os.environ.get("GUAI_LLM_MODEL", ""), "timeout": 30},
    # 语义检索（embedding）：写入时后台向量化，检索时与关键词结果融合
    "semantic": True,
    "embed": {"api_key": os.environ.get("GUAI_EMBED_API_KEY", ""),
              "base_url": os.environ.get("GUAI_EMBED_BASE_URL", ""),
              "model": os.environ.get("GUAI_EMBED_MODEL", ""), "timeout": 30, "dim": 1024},
    "mcp_token": os.environ.get("GUAI_MCP_TOKEN", ""),
    "dashboard_password": os.environ.get("GUAI_DASHBOARD_PASSWORD", ""),
}


def load_config() -> dict[str, Any]:
    cfg = {k: (json.loads(json.dumps(v)) if isinstance(v, (dict, list)) else v)
           for k, v in DEFAULT_CONFIG.items()}
    if CONFIG_PATH.exists():
        try:
            cfg.update(yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {})
        except Exception:
            pass
    return cfg


def save_config(cfg: dict[str, Any]) -> None:
    """写回 config.yaml。文件在 data/ 下，只进私密库。"""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        yaml.safe_dump(cfg, allow_unicode=True, sort_keys=False), encoding="utf-8")


CFG = load_config()

# ─────────────────────────── 数据库 ───────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS buckets (
    id              TEXT PRIMARY KEY,
    title           TEXT NOT NULL DEFAULT '',
    content         TEXT NOT NULL,
    domain          TEXT NOT NULL DEFAULT '未分类',
    tags            TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL DEFAULT 'event',
    importance      REAL NOT NULL DEFAULT 50,
    importance_base REAL NOT NULL DEFAULT 50,
    pinned          INTEGER NOT NULL DEFAULT 0,
    resolved        INTEGER NOT NULL DEFAULT 0,
    digested        INTEGER NOT NULL DEFAULT 0,
    protected_until INTEGER NOT NULL DEFAULT 0,
    protect_count   INTEGER NOT NULL DEFAULT 0,
    concept_key     TEXT NOT NULL DEFAULT '',
    concept_value   TEXT NOT NULL DEFAULT '',
    superseded_by   TEXT NOT NULL DEFAULT '',
    valid_to        INTEGER NOT NULL DEFAULT 0,
    emotion_capture TEXT NOT NULL DEFAULT '{}',
    emotion_last    TEXT NOT NULL DEFAULT '{}',
    emotion_history TEXT NOT NULL DEFAULT '[]',
    created_at      INTEGER NOT NULL,
    last_active     INTEGER NOT NULL,
    recall_count    INTEGER NOT NULL DEFAULT 0,
    source          TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_buckets_imp ON buckets(importance DESC);
CREATE INDEX IF NOT EXISTS idx_buckets_domain ON buckets(domain);
CREATE INDEX IF NOT EXISTS idx_buckets_kind ON buckets(kind);
CREATE TABLE IF NOT EXISTS audit (
    id TEXT PRIMARY KEY, at INTEGER NOT NULL, actor TEXT NOT NULL,
    action TEXT NOT NULL, bucket_id TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS inject_log ( at INTEGER NOT NULL, bucket_id TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY, at INTEGER NOT NULL, op TEXT NOT NULL,
    payload TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meta ( k TEXT PRIMARY KEY, v TEXT NOT NULL );
CREATE TABLE IF NOT EXISTS embeddings (
    bucket_id TEXT PRIMARY KEY,
    dim       INTEGER NOT NULL,
    vec       BLOB NOT NULL,
    model     TEXT NOT NULL DEFAULT '',
    at        INTEGER NOT NULL
);
"""

FTS_TRI = """
CREATE VIRTUAL TABLE IF NOT EXISTS buckets_fts USING fts5(
    title, content, tags, domain, content='buckets', content_rowid='rowid', tokenize='trigram'
);
"""
FTS_FALLBACK = """
CREATE VIRTUAL TABLE IF NOT EXISTS buckets_fts USING fts5(
    title, content, tags, domain, content='buckets', content_rowid='rowid'
);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def tx():
    conn = db()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with tx() as c:
        c.executescript(SCHEMA)
        try:
            c.executescript(FTS_TRI)
        except sqlite3.OperationalError:
            c.executescript(FTS_FALLBACK)
        try:
            c.execute("ALTER TABLE buckets ADD COLUMN expire_at INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        c.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('schema','6.3')")


init_db()

# ─────────────────────────── 小工具 ───────────────────────────


def now_ts() -> int:
    return int(time.time())


def uid() -> str:
    return uuid.uuid4().hex[:12]


def est_tokens(text: str) -> int:
    cjk = len(re.findall(r"[\u4e00-\u9fff\u3040-\u30ff]", text))
    other = max(0, len(text) - cjk)
    return int(cjk * 1.6 + other / 4) + 1


def slug(text: str, n: int = 40) -> str:
    s = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", text).strip("-")
    return s[:n] or "bucket"


def parse_emotion(e: Any) -> dict[str, float]:
    """接受 dict 或 JSON 字符串；解析失败退回中性。"""
    if isinstance(e, str):
        try:
            e = json.loads(e or "{}")
        except Exception:
            e = {}
    if isinstance(e, dict):
        try:
            return {"valence": float(e.get("valence", 0.5)), "arousal": float(e.get("arousal", 0.3))}
        except Exception:
            pass
    return {"valence": 0.5, "arousal": 0.3}


def emo_version(raw: str | dict, default: int = 0) -> int:
    if isinstance(raw, dict):
        try:
            return int(raw.get("version", default))
        except Exception:
            return default
    try:
        return int(json.loads(raw or "{}").get("version", default))
    except Exception:
        return default


def audit(actor: str, action: str, bucket_id: str = "", detail: str = "",
          conn: sqlite3.Connection | None = None) -> None:
    """写审计。调用方已在事务里时必须传 conn——否则新连接会被外层写锁堵死。"""
    sql = "INSERT INTO audit(id,at,actor,action,bucket_id,detail) VALUES(?,?,?,?,?,?)"
    args = (uid(), now_ts(), actor, action, bucket_id, detail)
    if conn is not None:
        conn.execute(sql, args)
        return
    with tx() as c:
        c.execute(sql, args)


def upsert_fts(c: sqlite3.Connection, bucket_id: str) -> None:
    try:
        c.execute("INSERT INTO buckets_fts(rowid,title,content,tags,domain) "
                  "SELECT rowid,title,content,tags,domain FROM buckets WHERE id=?", (bucket_id,))
    except sqlite3.OperationalError:
        pass

# ─────────────────────────── 权重引擎 ───────────────────────────


def effective_importance(row: dict[str, Any], at: Optional[int] = None) -> float:
    base = float(row.get("importance", 50))
    if row.get("pinned") or base >= 100:
        return 100.0
    if row.get("superseded_by"):
        return 0.0
    idle = ((at or now_ts()) - int(row.get("last_active", now_ts()))) / 86400.0
    no_decay = float(CFG["no_decay_days"])
    if idle <= no_decay:
        return round(base, 2)
    rate = float(CFG["decay_high"] if base >= 40 else CFG["decay_low"])
    return round(base * (rate ** ((idle - no_decay) / 30.0)), 2)


def touch_bucket(c: sqlite3.Connection, bucket_id: str, mentioned: bool = False) -> None:
    row = c.execute("SELECT * FROM buckets WHERE id=?", (bucket_id,)).fetchone()
    if not row:
        return
    r = dict(row)
    t = now_ts()
    c.execute("UPDATE buckets SET last_active=? WHERE id=?", (t, bucket_id))
    if not mentioned:
        return
    if r["pinned"] or r["importance"] >= 100:
        return
    base = min(float(r["importance_base"]) + 8.0, float(CFG["auto_cap"]))
    c.execute("UPDATE buckets SET importance=?, importance_base=? WHERE id=?", (base, base, bucket_id))
    if base >= 79.5:
        n = int(r["protect_count"])
        days = CFG["protect_days"][min(n, len(CFG["protect_days"]) - 1)]
        if days:
            c.execute("UPDATE buckets SET protect_count=?, protected_until=? WHERE id=?",
                      (n + 1, t + int(days) * 86400, bucket_id))


def apply_decay_all(at: Optional[int] = None) -> int:
    t = at or now_ts()
    n = 0
    with tx() as c:
        rows = c.execute("SELECT * FROM buckets WHERE pinned=0 AND importance<100 AND superseded_by=''").fetchall()
        for row in rows:
            eff = effective_importance(dict(row), t)
            if abs(eff - float(row["importance"])) > 0.01:
                c.execute("UPDATE buckets SET importance=? WHERE id=?", (eff, row["id"]))
                n += 1
            if row["protected_until"] and row["protected_until"] < t:
                c.execute("UPDATE buckets SET protected_until=0 WHERE id=?", (row["id"],))
    return n

# ─────────────────────────── 冲突 ───────────────────────────


class NeedsConfirm(Exception):
    """100% 记忆（pinned）的修改需要先征得用户同意。"""


ADD, UPDATE, DELETE, NOOP = "ADD", "UPDATE", "DELETE", "NOOP"


def resolve_conflict(c: sqlite3.Connection, concept_key: str, concept_value: str,
                     actor: str, confirm: bool = False) -> tuple[str, Optional[sqlite3.Row]]:
    if not concept_key:
        return ADD, None
    row = c.execute(
        "SELECT * FROM buckets WHERE concept_key=? AND superseded_by='' "
        "ORDER BY last_active DESC LIMIT 1", (concept_key,)).fetchone()
    if not row:
        return ADD, None
    if row["concept_value"] == concept_value:
        return NOOP, row
    if row["pinned"]:
        # 100% 记忆只能新增、修改，不能被覆盖/作废（那等于删除）
        return ADD, row
    return UPDATE, row


def guard_modify(row: sqlite3.Row, actor: str, confirm: bool = False) -> None:
    """pinned 记忆：AI 与用户都能改，但必须先征得用户同意（confirm=true）。"""
    if row["pinned"] and not confirm:
        raise NeedsConfirm(
            "这条是 100% 记忆。改之前请先向用户说明要改什么，得到同意后带 confirm=true 重试。"
        )

# ─────────────────────────── 情绪两档 ───────────────────────────


def apply_emotion_update(c: sqlite3.Connection, row: sqlite3.Row,
                         new: dict[str, float], actor: str) -> None:
    """capture (v0) 永不覆盖；last 可自由覆盖，version 只作「第几次」计数。

    被覆盖的旧 last 压进 emotion_history（保留最近 5 条）。
    """
    last_raw = row["emotion_last"] or "{}"
    last = parse_emotion(last_raw)
    last_v = emo_version(last_raw, default=1)
    new_v = max(emo_version(new, default=last_v + 1), last_v + 1)  # 不允许倒退
    new_last = {**parse_emotion(new), "version": new_v}

    hist = json.loads(row["emotion_history"] or "[]")
    hist.append(last)
    hist = hist[-5:]

    c.execute("UPDATE buckets SET emotion_last=?, emotion_history=? WHERE id=?",
              (json.dumps(new_last, ensure_ascii=False), json.dumps(hist, ensure_ascii=False), row["id"]))
    audit(actor, "EMO_UPDATE", row["id"], json.dumps(new_last, ensure_ascii=False), conn=c)

# ─────────────────────────── Markdown 镜像 ───────────────────────────


def mirror_to_vault(row: sqlite3.Row) -> None:
    if not CFG.get("mirror_enabled", True):
        return
    r = dict(row)
    d = VAULT / slug(r["domain"], 30)
    d.mkdir(parents=True, exist_ok=True)
    cap = parse_emotion(r["emotion_capture"])
    last_raw = r["emotion_last"] or "{}"
    last = parse_emotion(last_raw)
    fm = {
        "id": r["id"], "title": r["title"], "domain": r["domain"], "tags": r["tags"],
        "kind": r["kind"], "importance": r["importance"], "pinned": bool(r["pinned"]),
        "resolved": bool(r["resolved"]),
        "emotion": {"first": {"valence": cap["valence"], "arousal": cap["arousal"], "version": 0},
                    "last": {**last, "version": emo_version(last_raw, default=1)}},
        "created": datetime.fromtimestamp(r["created_at"], timezone.utc).isoformat(),
    }
    body = ("---\n" + yaml.safe_dump(fm, allow_unicode=True, sort_keys=False) +
            "---\n\n# " + (r["title"] or "(无标题)") + "\n\n" + r["content"] + "\n")
    # 同一个桶只保留一份镜像：title/domain 变了路径会变，旧文件要清掉
    for old in VAULT.rglob(f"*-{r['id'][:6]}.md"):
        try:
            old.unlink()
        except Exception:
            pass
    (d / f"{slug(r['title'] or r['id'])}-{r['id'][:6]}.md").write_text(body, encoding="utf-8")

# ─────────────────────────── 写入 ───────────────────────────

VALID_KINDS = {"event", "feel", "plan", "letter", "persona", "anchor", "i", "you", "them", "dream"}


def write_bucket(*, content: str, title: str = "", domain: str = "未分类", tags: str = "",
                 kind: str = "event", category: str = "life", importance: Optional[float] = None,
                 actor: str = "ai", emotion: Any = None, concept_key: str = "",
                 concept_value: str = "", pinned: bool = False, source: str = "",
                 confirm: bool = False, expire_at: int = 0) -> dict[str, Any]:
    if kind not in VALID_KINDS:
        kind = "event"
    if not content.strip():
        raise ValueError("content 不能为空")
    if pinned and not confirm:
        raise NeedsConfirm("要把一条记忆钉成 100%，请先征得用户同意，再带 confirm=true 重试。")

    emo = parse_emotion(emotion)
    base = importance if importance is not None else float(CFG["initial"].get(category, 50))
    if kind == "persona" or pinned:
        base = 100.0
    base = min(max(0.0, base), 100.0)
    t = now_ts()
    bid = uid()
    cap = {**emo, "version": 0}          # 第一次，永不覆盖
    last_first = {**emo, "version": 1}   # 首次回忆

    with tx() as c:
        action, target = resolve_conflict(c, concept_key, concept_value, actor, confirm)
        if target is not None and action == UPDATE:
            guard_modify(target, actor, confirm)
            c.execute("UPDATE buckets SET superseded_by=?, valid_to=? WHERE id=?",
                      (bid, t, target["id"]))
            audit(actor, "SUPERSEDE", target["id"], f"by {bid}", conn=c)
        c.execute(
            "INSERT INTO buckets(id,title,content,domain,tags,kind,importance,importance_base,"
            "pinned,concept_key,concept_value,emotion_capture,emotion_last,emotion_history,"
            "created_at,last_active,source,expire_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (bid, (title or content[:24]).replace("\n", " "), content, domain, tags, kind,
             base, base, 1 if pinned else 0, concept_key, concept_value,
             json.dumps(cap, ensure_ascii=False), json.dumps(last_first, ensure_ascii=False),
             json.dumps([], ensure_ascii=False), t, t, source, expire_at))
        upsert_fts(c, bid)
        if CFG.get("storage") == "cloud":
            c.execute("INSERT INTO outbox(id,at,op,payload) VALUES(?,?,?,?)",
                      (uid(), t, "upsert", json.dumps({"id": bid, "content": content}, ensure_ascii=False)))
        row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
        audit(actor, "WRITE", bid, f"kind={kind} base={base}", conn=c)

    mirror_to_vault(row)
    _post_process(bid, content, title, domain, tags, kind, category, base)
    return dict(row)

# ─────────────────────────── LLM 打标 / 向量检索 ───────────────────────────


def _http_json(url: str, payload: dict, api_key: str, timeout: int = 30) -> dict:
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + api_key})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


TAG_PROMPT = """你是记忆库的打标器。给下面这条记忆判断属性，只输出一个 JSON 对象，不要任何解释：
{"domain":"领域名（1-6字）","tags":"标签，逗号分隔","kind":"event","category":"life"}
说明：kind 取值 event/feel/plan/persona/anchor/letter 之一；category 取 life/work/major 之一。
生活、身体、情绪、约定、日常、吃喝作息 → life；工作、项目、技术、排障 → work；重大转折、关系定位 → major。
记忆："""


def auto_tag(content: str, title: str = "") -> Optional[dict]:
    """调 LLM 自动打标。失败返回 None，调用方回退到手动值。"""
    cfg = CFG.get("llm") or {}
    if not CFG.get("auto_tag") or not cfg.get("api_key") or not cfg.get("base_url"):
        return None
    try:
        data = _http_json(
            cfg["base_url"].rstrip("/") + "/chat/completions",
            {"model": cfg["model"], "temperature": 0.2, "max_tokens": 200,
             "messages": [{"role": "system", "content": "你只输出 JSON。"},
                          {"role": "user", "content": TAG_PROMPT + (title + "\n" if title else "") + content[:1500]}]},
            cfg["api_key"], int(cfg.get("timeout", 30)))
        txt = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
        m = re.search(r"\{.*\}", txt, re.S)
        obj = json.loads(m.group(0)) if m else {}
        if obj.get("kind") not in VALID_KINDS:
            obj["kind"] = "event"
        if obj.get("category") not in ("life", "work", "major", "persona", "must"):
            obj["category"] = "life"
        if not obj.get("domain"):
            obj["domain"] = "未分类"
        return obj
    except Exception:
        return None


def embed_text(text: str) -> Optional[list]:
    cfg = CFG.get("embed") or {}
    if not CFG.get("semantic") or not cfg.get("api_key") or not cfg.get("base_url"):
        return None
    try:
        data = _http_json(
            cfg["base_url"].rstrip("/") + "/embeddings",
            {"model": cfg["model"], "input": text[:2000]},
            cfg["api_key"], int(cfg.get("timeout", 30)))
        vec = (data.get("data") or [{}])[0].get("embedding")
        return list(vec) if vec else None
    except Exception:
        return None


def _cosine(a: list, b: list) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


def store_embedding(bucket_id: str, text: str) -> bool:
    vec = embed_text(text)
    if not vec:
        return False
    with tx() as c:
        c.execute("INSERT OR REPLACE INTO embeddings(bucket_id,dim,vec,model,at) VALUES(?,?,?,?,?)",
                  (bucket_id, len(vec), struct.pack(f"{len(vec)}f", *vec),
                   (CFG.get("embed") or {}).get("model", ""), now_ts()))
    return True


def semantic_search(q: str, limit: int = 20) -> list:
    """返回 [(余弦相似度, bucket_id)]，按相似度降序。"""
    vec = embed_text(q)
    if not vec:
        return []
    conn = db()
    try:
        rows = conn.execute("SELECT bucket_id, dim, vec FROM embeddings").fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        arr = struct.unpack(f"{r['dim']}f", r["vec"])
        out.append((_cosine(vec, list(arr)), r["bucket_id"]))
    out.sort(key=lambda x: x[0], reverse=True)
    return out[:limit]


def _post_process(bid: str, content: str, title: str, domain: str, tags: str,
                  kind: str, category: str, importance: float) -> None:
    """写入后后台做两件事：自动打标 + 向量化。失败不影响已写入的记忆。"""

    def run():
        try:
            obj = auto_tag(content, title)
            if obj:
                cat2 = obj.get("category")
                new_imp = importance
                # 用户没手动给分时，才采用 LLM 判定的类别来定初始权重
                if cat2 and abs(float(importance) - float(CFG["initial"].get(category, 50))) < 0.01:
                    new_imp = float(CFG["initial"].get(cat2, importance))
                new_kind = obj.get("kind") or kind
                if kind == "dream":
                    new_kind = "dream"      # 梦的标记不能被打标覆盖
                with tx() as c:
                    c.execute("UPDATE buckets SET domain=?, tags=?, kind=?, importance=?, importance_base=? WHERE id=?",
                              (obj.get("domain") or domain, obj.get("tags") or tags,
                               new_kind, new_imp, new_imp, bid))
                conn = db()
                try:
                    row = conn.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
                finally:
                    conn.close()
                if row:
                    mirror_to_vault(row)   # 打标后同步 Markdown，否则真源还是旧值
        except Exception:
            pass
        try:
            store_embedding(bid, (title or "") + "\n" + content)
        except Exception:
            pass

    threading.Thread(target=run, daemon=True).start()


def hard_delete(bucket_id: str) -> bool:
    """物理删除：数据库行 + Markdown 镜像 + 向量。只有面板（人类）能调。"""
    conn = db()
    try:
        row = conn.execute("SELECT id FROM buckets WHERE id=?", (bucket_id,)).fetchone()
    finally:
        conn.close()
    if not row:
        return False
    for old in VAULT.rglob(f"*-{bucket_id[:6]}.md"):
        try:
            old.unlink()
        except Exception:
            pass
    with tx() as c:
        c.execute("DELETE FROM buckets WHERE id=?", (bucket_id,))
        c.execute("DELETE FROM embeddings WHERE bucket_id=?", (bucket_id,))
        try:
            c.execute("INSERT INTO buckets_fts(buckets_fts) VALUES('rebuild')")
        except sqlite3.OperationalError:
            pass
    return True


def purge_expired() -> int:
    """清掉显式设置了过期时间的记忆（梦现在不走这条）。"""
    t = now_ts()
    conn = db()
    try:
        ids = [r["id"] for r in conn.execute(
            "SELECT id FROM buckets WHERE expire_at>0 AND expire_at<?").fetchall()]
    finally:
        conn.close()
    n = 0
    for bid in ids:
        if hard_delete(bid):
            n += 1
    return n


def trim_dreams(keep: int = 100) -> int:
    """梦只保留最近 keep 条，超出的从最早的开始删。"""
    conn = db()
    try:
        rows = conn.execute(
            "SELECT id FROM buckets WHERE kind='dream' ORDER BY created_at DESC, rowid DESC").fetchall()
    finally:
        conn.close()
    extra = [r["id"] for r in rows[max(0, int(keep)):]]
    n = 0
    for bid in extra:
        if hard_delete(bid):
            n += 1
    return n


# ─────────────────────────── 检索 ───────────────────────────


def search(q: str, domain: str = "", limit: int = 20) -> list[dict[str, Any]]:
    conn = db()
    try:
        where, params = ["b.superseded_by=''"], []
        if domain:
            where.append("b.domain=?")
            params.append(domain)
        wsql = " AND " + " AND ".join(where)
        if q.strip():
            rows = []
            qs = q.strip()
            if len(qs) >= 3:
                try:
                    rows = conn.execute(
                        f"SELECT b.*, bm25(buckets_fts) AS rank FROM buckets_fts "
                        f"JOIN buckets b ON b.rowid = buckets_fts.rowid "
                        f"WHERE buckets_fts MATCH ? {wsql} ORDER BY rank LIMIT ?",
                        [qs, *params, limit]).fetchall()
                except Exception:
                    rows = []
            if not rows:
                # 中文短词（<3 字）或 FTS 无命中：LIKE 兜底（trigram 至少需要 3 字符）
                rows = conn.execute(
                    f"SELECT b.* FROM buckets b WHERE "
                    f"(b.content LIKE ? OR b.title LIKE ? OR b.tags LIKE ?) {wsql} "
                    f"ORDER BY b.importance DESC LIMIT ?",
                    [f"%{qs}%", f"%{qs}%", f"%{qs}%", *params, limit]).fetchall()
        else:
            rows = conn.execute(
                f"SELECT b.* FROM buckets b WHERE 1=1 {wsql} ORDER BY b.importance DESC LIMIT ?",
                [*params, limit]).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["effective"] = effective_importance(d)
            d["semantic"] = None
            out.append(d)
        # 语义融合：按「意思」命中的排在前面
        if q.strip() and CFG.get("semantic"):
            try:
                hits = [(sc, bid) for sc, bid in semantic_search(q, limit=limit) if sc >= 0.35]
                if hits:
                    have = {d["id"] for d in out}
                    want = [bid for sc, bid in hits if bid not in have]
                    sem_items = []
                    if want:
                        ph = ",".join("?" * len(want))
                        c2 = db()
                        try:
                            erows = c2.execute(f"SELECT * FROM buckets WHERE id IN ({ph})", want).fetchall()
                        finally:
                            c2.close()
                        emap = {r["id"]: dict(r) for r in erows}
                        for sc, bid in hits:
                            if bid in emap:
                                d = emap[bid]
                                d["effective"] = effective_importance(d)
                                d["semantic"] = round(sc, 3)
                                sem_items.append(d)
                    smap = {bid: sc for sc, bid in hits}
                    for d in out:
                        if d["id"] in smap:
                            d["semantic"] = round(smap[d["id"]], 3)
                    out = sem_items + out
            except Exception:
                pass
        return out
    finally:
        conn.close()

# ─────────────────────────── 注入分层 ───────────────────────────


def recent_injected_ids() -> set[str]:
    n = int(CFG["inject"]["recent_window"])
    conn = db()
    try:
        return {r["bucket_id"] for r in conn.execute(
            "SELECT bucket_id FROM inject_log ORDER BY at DESC LIMIT ?", (n * 50,)).fetchall()}
    finally:
        conn.close()


def log_injection(ids: list[str]) -> None:
    t = now_ts()
    with tx() as c:
        for i in ids:
            c.execute("INSERT INTO inject_log(at,bucket_id) VALUES(?,?)", (t, i))


def _l2_band(score: float) -> str:
    """返回档位名（不是配额数）——配额数会重复，用数字反查会串档。"""
    if score >= 79:
        return "79-80"
    if score >= 70:
        return "70-78"
    if score >= 60:
        return "60-69"
    return "50-59"


def build_injection(at: Optional[int] = None, dry: bool = False) -> dict[str, Any]:
    t = at or now_ts()
    conn = db()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM buckets WHERE superseded_by='' AND digested=0 AND kind!='dream'").fetchall()]
    finally:
        conn.close()
    for r in rows:
        r["effective"] = effective_importance(r, t)

    recent = recent_injected_ids()
    l0 = [r for r in rows if r["effective"] >= 100 or r["kind"] == "persona"]
    l1 = [r for r in rows if 80 <= r["effective"] < 100]
    l2 = [r for r in rows if 30 <= r["effective"] < 80]
    l3 = [r for r in rows if r["effective"] < 30]

    def prio(r: dict) -> float:
        s = r["effective"]
        if r["id"] in recent:
            s *= float(CFG["inject"]["recent_penalty"])
        if r["resolved"]:
            s -= 15
        if r["pinned"]:
            s += 50
        return s

    for lst in (l0, l1, l2):
        lst.sort(key=prio, reverse=True)

    picked = l0 + l1
    domain_count: dict[str, int] = {}
    quota_left = {k: int(v) for k, v in CFG["inject"]["l2_quota"].items()}
    cap = len(l0) + len(l1) + int(CFG["inject"]["l2_pick"])
    for r in l2:
        if len(picked) >= cap:
            break
        band = _l2_band(r["effective"])
        if quota_left.get(band, 0) <= 0:
            continue
        if domain_count.get(r["domain"], 0) >= int(CFG["inject"]["same_domain_max"]):
            continue
        picked.append(r)
        domain_count[r["domain"]] = domain_count.get(r["domain"], 0) + 1
        quota_left[band] -= 1

    if l3:
        l3s = sorted(l3, key=prio, reverse=True)
        picked.append(l3s[0])
        pool = [x for x in l3 if x["id"] != l3s[0]["id"]]
        if pool:
            picked.append(random.choice(pool))

    budget = int(CFG["inject"]["l0_tokens"]) + int(CFG["inject"]["l1_tokens"])
    max_chars = int(CFG["inject"]["max_item_chars"])
    out, used = [], 0
    for r in picked:
        text = (r["title"] + "\n" + r["content"])[:max_chars]
        cost = est_tokens(text)
        if used + cost > budget:
            continue
        used += cost
        cap_raw, last_raw = r["emotion_capture"] or "{}", r["emotion_last"] or "{}"
        out.append({
            "id": r["id"], "title": r["title"], "content": text,
            "domain": r["domain"], "kind": r["kind"],
            "importance": r["importance"], "effective": r["effective"],
            "pinned": bool(r["pinned"]),
            "emotion_first": {**parse_emotion(cap_raw), "version": 0},
            "emotion_last": {**parse_emotion(last_raw), "version": emo_version(last_raw, default=1)},
        })
    if not dry and out:
        log_injection([o["id"] for o in out])
    return {"layers": {"L0": len(l0), "L1": len(l1), "L2": len(l2), "L3": len(l3)},
            "picked": out, "tokens_used": used, "budget": budget}

# ─────────────────────────── 做梦三合一 ───────────────────────────


def dream(window_hours: int = 48, dry: bool = False) -> dict[str, Any]:
    t = now_ts()
    since = t - window_hours * 3600
    decayed = 0 if dry else apply_decay_all(t)
    trimmed = 0
    conn = db()
    try:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM buckets WHERE last_active>=? AND pinned=0 AND superseded_by=''",
            (since,)).fetchall()]
        plans = [dict(r) for r in conn.execute(
            "SELECT * FROM buckets WHERE kind='plan' AND resolved=0 AND superseded_by=''").fetchall()]
    finally:
        conn.close()

    def avg(em):
        if not em:
            return {"valence": 0.5, "arousal": 0.3}
        n = len(em)
        return {"valence": sum(x["valence"] for x in em) / n, "arousal": sum(x["arousal"] for x in em) / n}

    firsts = [parse_emotion(r["emotion_capture"]) for r in rows]
    lasts = [parse_emotion(r["emotion_last"]) for r in rows]

    seeds = sorted(rows, key=lambda r: r["last_active"], reverse=True)[:3]
    lines = [f"- {r['title']}：{r['content'][:60]}" for r in seeds]
    dream_text = "今晚翻过的几页：\n" + ("\n".join(lines) if lines else "- 空")
    created: list[str] = []
    # 梦的新记忆：情绪取 capture (v0) —— 梦是「想起最初那一下」
    if seeds and not dry:
        for r in seeds[:2]:
            created.append(write_bucket(
                content=f"梦里又回到「{r['title']}」。醒来只剩一点余温，比昨天的淡。",
                title=f"梦·{r['title'][:16]}", domain="梦", kind="dream", category="life",
                actor="ai", emotion=parse_emotion(r["emotion_capture"]),
                source=f"dream:{r['id']}")["id"])
    # 补向量：把还没向量化的记忆补上（每次最多 20 条，失败不影响做梦）
    backfilled = 0
    if CFG.get("semantic"):
        try:
            c3 = db()
            try:
                missing = [r["id"] for r in c3.execute(
                    "SELECT b.id FROM buckets b LEFT JOIN embeddings e ON e.bucket_id=b.id "
                    "WHERE e.bucket_id IS NULL AND b.superseded_by='' LIMIT 20").fetchall()]
            finally:
                c3.close()
            for mid in missing:
                c4 = db()
                try:
                    rr = c4.execute("SELECT title, content FROM buckets WHERE id=?", (mid,)).fetchone()
                finally:
                    c4.close()
                if rr and store_embedding(mid, (rr["title"] or "") + "\n" + (rr["content"] or "")):
                    backfilled += 1
        except Exception:
            pass

    if not dry:
        trimmed = trim_dreams(int(CFG.get("dream_keep", 100)))

    return {"decayed": decayed, "trimmed_dreams": trimmed, "vector_backfilled": backfilled,
            "window_buckets": len(rows), "active_plans": len(plans),
            "emotion_settled": {"first": avg(firsts), "current": avg(lasts)},
            "dream": dream_text, "dream_buckets": created, "dry_run": dry}

# ─────────────────────────── FastAPI ───────────────────────────

app = FastAPI(title="归屿记忆系统", version="6.2.0")


def check_mcp_auth(authorization: Optional[str]) -> None:
    token = CFG.get("mcp_token") or ""
    if not token:
        return
    if not authorization or authorization.split(" ", 1)[-1] != token:
        raise HTTPException(401, "bad mcp token")


def check_dash(authorization: Optional[str], x_actor: Optional[str]) -> str:
    """面板接口一律视为人类操作——用户身份只从这里来，MCP 不能自称。"""
    pw = CFG.get("dashboard_password") or ""
    if pw and (authorization or "").split(" ", 1)[-1] != pw:
        raise HTTPException(401, "需要 Dashboard 密码")
    return "user"


@app.get("/health")
def health() -> dict[str, Any]:
    with tx() as c:
        n = c.execute("SELECT COUNT(*) n FROM buckets").fetchone()["n"]
        p = c.execute("SELECT COUNT(*) n FROM buckets WHERE pinned=1").fetchone()["n"]
    return {"status": "ok", "buckets": n, "pinned": p, "version": "6.2.0", "ts": now_ts()}


@app.get("/api/stats")
def api_stats(authorization: Optional[str] = Header(None), x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    with tx() as c:
        total = c.execute("SELECT COUNT(*) n FROM buckets WHERE superseded_by=''").fetchone()["n"]
        pinned = c.execute("SELECT COUNT(*) n FROM buckets WHERE pinned=1").fetchone()["n"]
        archived = c.execute("SELECT COUNT(*) n FROM buckets WHERE superseded_by!=''").fetchone()["n"]
        plans = c.execute("SELECT COUNT(*) n FROM buckets WHERE kind='plan' AND resolved=0").fetchone()["n"]
        by_domain = [dict(r) for r in c.execute(
            "SELECT domain, COUNT(*) n FROM buckets WHERE superseded_by='' GROUP BY domain ORDER BY n DESC")]
        by_kind = [dict(r) for r in c.execute(
            "SELECT kind, COUNT(*) n FROM buckets WHERE superseded_by='' GROUP BY kind ORDER BY n DESC")]
    return {"total": total, "pinned": pinned, "archived": archived, "plans": plans,
            "by_domain": by_domain, "by_kind": by_kind, "storage": CFG.get("storage")}


@app.get("/api/buckets")
def api_buckets(q: str = "", domain: str = "", kind: str = "", page: int = 1, size: int = 20,
                authorization: Optional[str] = Header(None), x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    size = max(1, min(size, 100))
    items = search(q, domain=domain, limit=size * page + size)
    if kind:
        items = [i for i in items if i["kind"] == kind]
    start = (page - 1) * size
    return {"items": items[start:start + size], "page": page, "size": size, "total": len(items)}


@app.get("/api/bucket/{bid}")
def api_bucket(bid: str, authorization: Optional[str] = Header(None),
               x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    with tx() as c:
        row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    d = dict(row)
    d["effective"] = effective_importance(d)
    d["emotion_first"] = {**parse_emotion(row["emotion_capture"]), "version": 0}
    d["emotion_last"] = {**parse_emotion(row["emotion_last"]),
                         "version": emo_version(row["emotion_last"], default=1)}
    return d


@app.post("/api/bucket/{bid}/touch")
def api_touch(bid: str, body: dict = Body(default={}), authorization: Optional[str] = Header(None),
              x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    actor = check_dash(authorization, x_actor)
    mentioned = bool(body.get("mentioned", True))
    with tx() as c:
        touch_bucket(c, bid, mentioned=mentioned)
        audit(actor, "TOUCH", bid, f"mentioned={mentioned}")
    return {"ok": True}


@app.post("/api/bucket/{bid}/pin")
def api_pin(bid: str, body: dict = Body(default={}), authorization: Optional[str] = Header(None),
            x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    actor = check_dash(authorization, x_actor)
    pin = bool(body.get("pinned", True))
    if not body.get("confirm", False):
        raise HTTPException(409, "改 pinned 前需要用户确认：带上 confirm=true 再请求")
    with tx() as c:
        row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        c.execute("UPDATE buckets SET pinned=?, importance=? WHERE id=?",
                  (1 if pin else 0, 100 if pin else float(row["importance_base"]), bid))
    audit(actor, "PIN" if pin else "UNPIN", bid)
    return {"ok": True}


@app.post("/api/bucket/{bid}/resolve")
def api_resolve(bid: str, body: dict = Body(default={}), authorization: Optional[str] = Header(None),
                x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    actor = check_dash(authorization, x_actor)
    val = bool(body.get("resolved", True))
    with tx() as c:
        row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        guard_modify(row, actor, bool(body.get("confirm", False)))
        c.execute("UPDATE buckets SET resolved=? WHERE id=?", (1 if val else 0, bid))
    audit(actor, "RESOLVE", bid)
    return {"ok": True}


@app.post("/api/bucket/{bid}/emotion")
def api_emotion(bid: str, body: dict = Body(...), authorization: Optional[str] = Header(None),
                x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    """生成新情绪标签并覆盖 last（version 自动 +1，旧 last 压入 history）。capture 不动。"""
    actor = check_dash(authorization, x_actor)
    with tx() as c:
        row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
        if not row:
            raise HTTPException(404, "not found")
        if row["pinned"] and not body.get("confirm", False):
            raise HTTPException(409, "这条是 100% 记忆，改情绪前需要用户确认（confirm=true）")
        apply_emotion_update(c, row, parse_emotion(body), actor)
    return {"ok": True}


@app.delete("/api/bucket/{bid}")
def api_delete(bid: str, authorization: Optional[str] = Header(None),
               x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    """删除记忆。只有面板（人类）走这条路；MCP 不提供任何删除工具。"""
    check_dash(authorization, x_actor)
    ok = hard_delete(bid)
    if ok:
        audit("user", "DELETE", bid, "via dashboard")
    return {"ok": ok}


@app.post("/api/breath")
def api_breath(body: dict = Body(default={}), authorization: Optional[str] = Header(None),
               x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    return build_injection(dry=bool(body.get("dry", True)))


@app.post("/api/dream")
def api_dream(body: dict = Body(default={}), authorization: Optional[str] = Header(None),
              x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    return dream(int(body.get("window_hours", 48)), bool(body.get("dry", True)))


@app.post("/api/config")
def api_config_save(body: dict = Body(...), authorization: Optional[str] = Header(None),
                    x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    """面板保存 LLM / Embedding 配置。写 config.yaml，保存后立即生效。"""
    global CFG
    check_dash(authorization, x_actor)
    cfg = load_config()
    changed = []
    for sec in ("llm", "embed"):
        if isinstance(body.get(sec), dict):
            cur = cfg.setdefault(sec, {})
            for k in ("api_key", "base_url", "model"):
                v = str(body[sec].get(k, "") or "").strip()
                if v:                       # 空值不覆盖已有配置
                    cur[k] = v
            changed.append(sec)
    for k in ("auto_tag", "semantic"):
        if k in body:
            cfg[k] = bool(body[k])
            changed.append(k)
    save_config(cfg)
    CFG = load_config()
    audit("user", "CONFIG", "", ",".join(changed))
    return {"ok": True, "saved": changed}


@app.get("/api/config")
def api_config(authorization: Optional[str] = Header(None), x_actor: Optional[str] = Header(None)) -> dict[str, Any]:
    check_dash(authorization, x_actor)
    cfg = load_config()
    for k in ("mcp_token", "dashboard_password"):
        if cfg.get(k):
            cfg[k] = "***"
    # 嵌套的 key 也要脱敏，否则面板能读到完整密钥
    for sec in ("llm", "embed"):
        if isinstance(cfg.get(sec), dict) and cfg[sec].get("api_key"):
            cfg[sec]["api_key"] = "***"
    return cfg


TOOLS = [
    {"name": "breath", "description": "睁眼：按 L0–L4 分层把当前最该浮现的记忆拿出来。",
     "inputSchema": {"type": "object", "properties": {"dry": {"type": "boolean"}}}},
    {"name": "breath_search", "description": "按关键词检索记忆（命中不加热度）。",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}, "domain": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["query"]}},
    {"name": "hold", "description": "记下一件事。category: persona/must/major/life/work。",
     "inputSchema": {"type": "object", "properties": {"content": {"type": "string"}, "title": {"type": "string"}, "domain": {"type": "string"}, "tags": {"type": "string"}, "category": {"type": "string"}, "emotion": {"type": "object"}, "concept_key": {"type": "string"}, "concept_value": {"type": "string"}}, "required": ["content"]}},
    {"name": "grow", "description": "整理一段长内容，自动拆成 2–6 条。",
     "inputSchema": {"type": "object", "properties": {"content": {"type": "string"}, "domain": {"type": "string"}, "category": {"type": "string"}}, "required": ["content"]}},
    {"name": "trace", "description": "改单条记忆元数据。pinned 只能 user 改，81–99 需人工。emotion 更新：capture(v0) 永不覆盖，last 可自由覆盖（version 只作计数）。",
     "inputSchema": {"type": "object", "properties": {"bucket_id": {"type": "string"}, "resolved": {"type": "integer"}, "pinned": {"type": "integer"}, "importance": {"type": "number"}, "emotion": {"type": "object"}}, "required": ["bucket_id"]}},
    {"name": "pulse", "description": "记忆库体检：条数、分布、待办。",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "plan", "description": "登记一条承诺/待办。",
     "inputSchema": {"type": "object", "properties": {"content": {"type": "string"}}, "required": ["content"]}},
    {"name": "dream", "description": "做梦：整理（跳过 pinned）+ 情绪结算 + 生成梦。",
     "inputSchema": {"type": "object", "properties": {"window_hours": {"type": "integer"}, "dry": {"type": "boolean"}}}},
    {"name": "feel", "description": "按关键词找回以前留下的感受。",
     "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}},
]


def call_tool(name: str, args: dict[str, Any], actor: str = "ai") -> Any:
    if name == "breath":
        return build_injection(dry=bool(args.get("dry", False)))
    if name == "breath_search":
        return {"items": search(args.get("query", ""), args.get("domain", ""), int(args.get("limit", 20)))}
    if name == "hold":
        return write_bucket(content=args.get("content", ""), title=args.get("title", ""),
                           domain=args.get("domain", "未分类"), tags=args.get("tags", ""),
                           category=args.get("category", "life"), emotion=args.get("emotion"),
                           concept_key=args.get("concept_key", ""), concept_value=args.get("concept_value", ""),
                           actor=actor, confirm=bool(args.get("confirm", False)))
    if name == "grow":
        raw = args.get("content", "")
        parts = [p.strip() for p in re.split(r"\n{2,}|(?<=[。！？])\s*\n", raw) if p.strip()]
        if len(parts) < 2:
            parts = [p for p in re.split(r"(?<=[。！？])", raw) if p.strip()]
        ids = [write_bucket(content=p, domain=args.get("domain", "未分类"),
                            category=args.get("category", "life"), actor=actor)["id"]
                for p in parts[:6] if len(p) >= 4]
        return {"created": ids, "count": len(ids)}
    if name == "trace":
        bid = args["bucket_id"]
        with tx() as c:
            row = c.execute("SELECT * FROM buckets WHERE id=?", (bid,)).fetchone()
            if not row:
                raise ValueError("bucket not found")
            confirm = bool(args.get("confirm", False))
            guard_modify(row, actor, confirm)
            sets, vals = [], []
            if "resolved" in args:
                sets.append("resolved=?"); vals.append(1 if args["resolved"] else 0)
            if "pinned" in args:
                if not confirm:
                    raise NeedsConfirm("改 pinned 状态前请先征得用户同意（带 confirm=true）。")
                sets.append("pinned=?"); vals.append(1 if args["pinned"] else 0)
            if "importance" in args:
                imp = float(args["importance"])
                if imp > 80 and not confirm:
                    raise NeedsConfirm("81–99 是人工档位，升分前请先征得用户同意（带 confirm=true）。")
                sets += ["importance=?", "importance_base=?"]; vals += [imp, imp]
            if "emotion" in args:
                apply_emotion_update(c, row, parse_emotion(args["emotion"]), actor)
            if sets:
                vals.append(bid)
                c.execute(f"UPDATE buckets SET {','.join(sets)} WHERE id=?", vals)
        audit(actor, "TRACE", bid, json.dumps(args, ensure_ascii=False)[:200])
        return {"ok": True}
    if name == "pulse":
        with tx() as c:
            total = c.execute("SELECT COUNT(*) n FROM buckets WHERE superseded_by=''").fetchone()["n"]
            pinned = c.execute("SELECT COUNT(*) n FROM buckets WHERE pinned=1").fetchone()["n"]
            arch = c.execute("SELECT COUNT(*) n FROM buckets WHERE superseded_by!=''").fetchone()["n"]
            plans = c.execute("SELECT COUNT(*) n FROM buckets WHERE kind='plan' AND resolved=0").fetchone()["n"]
            outbox = c.execute("SELECT COUNT(*) n FROM outbox WHERE sent=0").fetchone()["n"]
        return {"total": total, "pinned": pinned, "archived": arch, "active_plans": plans,
                "outbox_pending": outbox, "storage": CFG.get("storage")}
    if name == "plan":
        return write_bucket(content=args.get("content", ""), kind="plan", domain="约定",
                            category="major", actor=actor)
    if name == "dream":
        return dream(int(args.get("window_hours", 48)), bool(args.get("dry", False)))
    if name == "feel":
        return {"items": [i for i in search(args.get("query", ""), limit=10) if i["kind"] == "feel"]}
    raise ValueError(f"unknown tool: {name}")


@app.post("/mcp")
async def mcp(req: Request, authorization: Optional[str] = Header(None),
              x_actor: Optional[str] = Header(None)):
    check_mcp_auth(authorization)
    actor = "ai"          # MCP 一律视为 AI，忽略客户端自称的身份
    try:
        body = await req.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "id": None,
                             "error": {"code": -32700, "message": "parse error"}}, status_code=400)

    def one(msg: dict) -> dict:
        mid, method, params = msg.get("id"), msg.get("method", ""), msg.get("params") or {}
        try:
            if method == "initialize":
                return {"jsonrpc": "2.0", "id": mid, "result": {
                    "protocolVersion": "2024-11-05", "capabilities": {"tools": {}},
                    "serverInfo": {"name": "guai-memory", "version": "6.2.0"}}}
            if method in ("notifications/initialized", "notifications/cancelled"):
                return {}
            if method == "tools/list":
                return {"jsonrpc": "2.0", "id": mid, "result": {"tools": TOOLS}}
            if method == "tools/call":
                out = call_tool(params.get("name", ""), params.get("arguments") or {}, actor=actor)
                return {"jsonrpc": "2.0", "id": mid,
                        "result": {"content": [{"type": "text", "text": json.dumps(out, ensure_ascii=False)}]}}
            if method == "ping":
                return {"jsonrpc": "2.0", "id": mid, "result": {}}
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"no method {method}"}}
        except NeedsConfirm as e:
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32004, "message": str(e),
                    "data": {"needs_confirmation": True}}}
        except PermissionError as e:
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32003, "message": str(e)}}
        except Exception as e:
            return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32000, "message": f"{type(e).__name__}: {e}"}}

    if isinstance(body, list):
        return JSONResponse([r for r in (one(m) for m in body) if r])
    return JSONResponse(one(body))


@app.get("/", response_class=HTMLResponse)
def index() -> Any:
    f = WEB_DIR / "index.html"
    if f.exists():
        return FileResponse(f)
    return HTMLResponse("<h1>归屿记忆</h1><p>web/index.html 未找到。</p>")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.environ.get("GUAI_HOST", "127.0.0.1"),
                port=int(os.environ.get("GUAI_PORT", "18002")))
