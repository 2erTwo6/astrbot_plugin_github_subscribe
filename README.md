# astrbot_plugin_github_subscribe

AstrBot 的 GitHub 订阅插件：让 AI 帮你搜索仓库、订阅仓库或开发者的 commit 与 Release，有新内容时由模型生成汇报并推送到你所在的会话。

## 功能

插件向 LLM 注册了 5 个工具，直接用自然语言对话即可使用：

| 工具 | 说明 |
| --- | --- |
| `github_search_repos` | 按关键词搜索仓库，返回 star 数、语言和简介 |
| `github_subscribe_repo` | 订阅仓库，追踪**所有分支**的 commit 和 Release |
| `github_subscribe_user` | 订阅开发者，追踪他向任意仓库（含组织仓库）的 commit 和 Release |
| `github_unsubscribe` | 取消订阅某个仓库或开发者 |
| `github_list_subscriptions` | 查看当前会话的订阅列表 |

订阅关系绑定到**当前会话**，不同群聊 / 私聊互不干扰。只有 AstrBot 管理员可以创建和取消订阅。

## 触发后会发生什么

有新 commit 或 Release 时，插件把结构化信息交给模型，由模型生成一段自然语言汇报再推送：

```
AstrBot 刚更新了，Soulter 在 master 分支提交了 3 个 commit，
主要是把版本号推到 4.29.2 并修复了 Windows 下的进程清理问题。
```

- commit 汇报包含：仓库、分支、提交者、commit 短哈希与标题
- Release 汇报包含：版本号、标题、更新日志（可配置截断长度）、链接
- 同一仓库一轮内的多条更新会**合并成一条消息**，不刷屏
- 模型不可用或超时会**降级为格式化文本**推送，不会漏消息

另外提供 `/ghsub list` 查看订阅、`/ghsub check` 立即检查一次。

## 配置

在 WebUI 的插件配置页填写：

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `github_token` | 空 | **强烈建议填写**，见下方限流说明 |
| `proxy` | 空 | HTTP 代理，如 `http://127.0.0.1:7890`，支持 http / https |
| `poll_interval` | 120 | 轮询间隔（秒） |
| `max_search_results` | 5 | 搜索结果条数 |
| `max_changelog_chars` | 1500 | 更新日志最多携带多少字符，0 表示不带正文 |
| `commit_detail_limit` | 10 | 单次汇报最多包含多少条 commit |
| `skip_tag_pattern` | `alpha` | 版本标签含这些关键词时跳过，逗号分隔 |
| `report_provider_id` | 空 | 生成汇报的模型，留空用会话当前模型 |
| `report_timeout` | 30 | 模型生成超时（秒） |

### 关于 GitHub Token

匿名访问 GitHub API 只有 **60 次/小时**，实测订阅几个仓库就会打满。建议填一个只读 Personal Access Token：

1. 打开 https://github.com/settings/tokens?type=beta
2. 生成 fine-grained token，**不需要勾选任何仓库权限**（公开数据只读即可）
3. 复制后填入 `github_token`

填写后额度提升到 5000 次/小时，额度吃紧时插件会自动拉长轮询间隔。

### 关于预发布版本

GitHub 的 `prerelease` 标记并不可靠——实测 AstrBot 的 `v4.29.0-beta.2` 被标记成了正式版。所以插件**不依赖该字段**，默认追踪所有版本，改用 `skip_tag_pattern` 按标签关键词过滤（默认跳过含 `alpha` 的标签，beta / rc 仍会推送）。留空该配置即追踪全部版本。

## 实现说明

- 数据源是 GitHub Events API，一次请求同时覆盖 PushEvent 和 ReleaseEvent，比分别轮询 commits / releases 省额度。
- 高活跃仓库上 PushEvent 的 `commits` 数组会被 GitHub 截断为空，此时插件自动补一次 `compare` 请求；同一分支的多条 push 会合并成一次 compare，避免请求量爆炸。
- 订阅时会**静默建立基线**，只推送订阅之后的新内容，不会把历史事件刷一遍。
- 每个订阅保存已处理的 event id 做去重；compare 请求失败时不会标记该事件为已处理，下一轮会重试，避免断网期间丢消息。

## 目录结构

```
astrbot_plugin_github_subscribe/
├── main.py             # 插件主逻辑
├── metadata.yaml       # 插件元信息
├── _conf_schema.json   # 配置 schema
├── requirements.txt    # aiohttp
├── README.md
└── tests/
    └── test_subscribe_logic.py   # 纯逻辑单测，不依赖网络
```

## 测试

```bash
python -m pytest astrbot_plugin_github_subscribe/tests/ -q
```
