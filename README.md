# 归屿记忆系统 (Guai Memory)

归屿 AI 伴侣 App 的记忆后端，实现 [docs/DESIGN_V6.md](docs/DESIGN_V6.md) 里我们定稿的九条设计。
单文件服务端 + 单页可视化面板 + 定期 git 备份。

**核心原则：记得住，改不了，懂轻重。**

## 文件结构

```
server.py            全部后端：SQLite+FTS5 存储、权重引擎、注入分层、冲突、情绪、做梦、MCP
web/index.html       可视化面板（暖灰白 + 雾绿，符合归屿 UI 配色）
requirements.txt     fastapi / uvicorn / pyyaml
.env.example         环境变量模板（数据目录、端口、鉴权）
deploy/
  guai-memory.service  systemd 单元
  nginx.conf           反代片段
  sync-git.sh          记忆 -> GitHub 定期备份脚本
  cron.txt             备份 + 每晚做梦的 crontab 行
docs/DESIGN_V6.md    设计定稿（可讲解版）
```

## 快速启动（本机）

```bash
git clone https://github.com/yvqing000/guai-memory
cd guai-memory
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# 编辑 .env：本机可不填密码；公网必须填 GUAI_DASHBOARD_PASSWORD + GUAI_MCP_TOKEN

.venv/bin/python server.py
# 面板 http://127.0.0.1:18002/    MCP http://127.0.0.1:18002/mcp
```

首次启动会在 `data/` 自动建 SQLite + 生成默认 `config.yaml`，并生成 `data/vault/` 记忆真源。

## 部署到服务器（长期在线）

```bash
sudo cp deploy/guai-memory.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now guai-memory
# nginx 里并入 deploy/nginx.conf 的 location /guai/ 段
crontab -e   # 加 deploy/cron.txt 里的两行（每小时备份 + 每晚做梦）
```

## 接入归屿 App

App 端把 MCP 端点指向 `https://<你的域名>/guai/mcp`，
带 `Authorization: Bearer <GUAI_MCP_TOKEN>`，`X-Actor: user` 表示以用户身份操作
（只有 user 身份能钉选 / 改 pinned / 升 81–99）。

MCP 工具：`breath` / `breath_search` / `hold` / `grow` / `trace` / `pulse` / `plan` / `dream` / `feel`。

## 验收

1. 说一句上周提过的事，不提醒它，它能自己接上（`breath` 能捞到）。
2. 同一句话，情绪 valence 高和低的两次，回得不一样（`emotion_capture` 保留第一次心情）。

## 还没做的（下一版）

- REST `/api/dream`（当前做梦走 MCP 工具）
- 语义向量检索（当前 FTS5 关键词；向量接口已留，接 embedding 后端即可）
- 双库真实云端同步（当前实现 outbox 队列 + local/cloud 开关，不双写）
- 81–99 升分审计界面
