"""GitHub 订阅插件。

通过 LLM 工具搜索 GitHub 仓库、订阅仓库或开发者的 commit 与 Release，
有新内容时把结构化信息交给模型生成汇报，再推送到订阅时所在的会话。

设计要点：

- 数据源统一走 GitHub Events API（``/repos/{owner}/{repo}/events`` 与
  ``/users/{login}/events/public``），一次请求就能同时覆盖 PushEvent 和
  ReleaseEvent，比分别轮询 commits / releases 两个接口省额度。
- PushEvent 的 ``commits`` 数组在高活跃仓库上会被 GitHub 截断为空（实测
  AstrBot 仓库的 PushEvent 只返回 before / head 两个 SHA），因此 commit
  名称必须再补一次 ``compare/{before}...{head}`` 请求。
- Events 流有窗口上限（最多 300 条 / 30 天），所以每个订阅都保存已处理
  的 event id 做去重，并在订阅时静默建立基线，避免刚订阅就被历史刷屏。
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import aiohttp

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star

GITHUB_API = "https://api.github.com"
USER_AGENT = "astrbot-plugin-github-subscribe/1.0"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=20)

# PushEvent 只在这些 ref 上代表一次真正的代码提交。
PUSH_REF_PREFIX = "refs/heads/"

REPORT_SYSTEM_PROMPT = (
    "你是一个 GitHub 订阅播报助手。用户订阅了某些仓库或开发者的动态，"
    "现在拿到了新的 commit 或 Release 信息，请用简洁自然的中文向用户汇报。\n"
    "要求：\n"
    "1. 先说清楚是哪个仓库、哪个开发者有新动静，是提交还是发版。\n"
    "2. commit 要提炼出实际改了什么，不要只重复原始 commit message，"
    "多个 commit 归纳成一两句重点，而不是逐条罗列。\n"
    "3. Release 要说明版本号、这个版本的主要变化，并给出链接。\n"
    "4. 语气像朋友在群里通知，不要用「以下是」「综上所述」这类套话，"
    "不要输出 Markdown 标题，控制在 5 行以内。\n"
    "5. 只能基于给出的信息汇报，不要编造没提供的改动。"
)


class GithubApi:
    """封装 GitHub REST 请求，处理代理、Token、限流与错误。"""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        token: str = "",
        proxy: str = "",
    ) -> None:
        """初始化 API 客户端。

        Args:
            session: 复用的 aiohttp 会话。
            token: GitHub Personal Access Token，留空则匿名访问。
            proxy: HTTP 代理地址，例如 ``http://127.0.0.1:7890``。
        """
        self.session = session
        self.token = token
        self.proxy = proxy
        # 剩余额度在每次响应后更新，用于决定是否放慢轮询。
        self.rate_limit_remaining: int | None = None
        self.rate_limit_reset: int = 0

    async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """发起一次 GET 请求并返回解析后的 JSON。

        Args:
            path: 相对 GitHub API 的路径，例如 ``/repos/a/b``。
            params: 查询参数。

        Returns:
            解析后的 JSON；404、限流或其他错误时返回 None。

        Raises:
            asyncio.CancelledError: 任务被取消时向上抛出，避免吞掉取消信号。
        """
        url = path if path.startswith("http") else f"{GITHUB_API}{path}"
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/vnd.github+json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            async with self.session.get(
                url,
                params=params,
                headers=headers,
                proxy=self.proxy or None,
                timeout=REQUEST_TIMEOUT,
            ) as resp:
                self._record_rate_limit(resp.headers)
                if resp.status >= 400:
                    # 403 在 GitHub 上通常代表触发限流，按额度耗尽处理。
                    if resp.status in (403, 429):
                        self.rate_limit_remaining = 0
                    return None
                return await resp.json(content_type=None)
        except asyncio.CancelledError:
            raise
        except Exception:
            # 网络抖动、超时、JSON 解析失败都视为本次无数据，下一轮再试。
            return None

    def _record_rate_limit(self, headers: Any) -> None:
        """记录响应头中的限流信息。

        Args:
            headers: aiohttp 响应头。
        """
        # 搜索接口走独立配额，但同样使用这两个头，取最小值即可覆盖两种情况。
        remaining = headers.get("X-RateLimit-Remaining")
        if remaining is not None and remaining.isdigit():
            value = int(remaining)
            self.rate_limit_remaining = (
                value
                if self.rate_limit_remaining is None
                else min(self.rate_limit_remaining, value)
            )
        reset = headers.get("X-RateLimit-Reset")
        if reset is not None and reset.isdigit():
            self.rate_limit_reset = int(reset)


def _trim(text: str, limit: int) -> str:
    """截断文本并在需要时补充提示。

    Args:
        text: 原始文本。
        limit: 最大保留字符数，<=0 表示全部丢弃。

    Returns:
        处理后的文本。
    """
    text = (text or "").strip()
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "…（已截断，完整内容见链接）"


class GithubSubscribePlugin(Star):
    """GitHub 订阅插件主类。"""

    def __init__(self, context: Context, config: dict | None = None) -> None:
        """读取配置并初始化订阅容器。

        Args:
            context: AstrBot 上下文。
            config: 插件配置（来自 _conf_schema.json）。
        """
        super().__init__(context)
        config = config or {}
        self.github_token = str(config.get("github_token", "") or "").strip()
        self.proxy = str(config.get("proxy", "") or "").strip()
        self.poll_interval = max(int(config.get("poll_interval", 120) or 120), 60)
        self.max_search_results = min(
            max(int(config.get("max_search_results", 5) or 5), 1),
            10,
        )
        self.max_changelog_chars = max(
            int(config.get("max_changelog_chars", 1500) or 0),
            0,
        )
        self.commit_detail_limit = min(
            max(int(config.get("commit_detail_limit", 10) or 10), 1),
            50,
        )
        self.report_provider_id = str(
            config.get("report_provider_id", "") or "",
        ).strip()
        self.report_timeout = max(int(config.get("report_timeout", 30) or 30), 10)
        # 标签关键词过滤，逗号分隔；留空表示所有版本都推送。
        raw_pattern = str(config.get("skip_tag_pattern", "alpha") or "")
        self.skip_tag_keywords = [
            item.strip().lower() for item in raw_pattern.split(",") if item.strip()
        ]

        # 结构: {unified_msg_origin: {"repos": {...}, "users": {...}}}
        self.subscriptions: dict[str, dict[str, dict[str, Any]]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._api: GithubApi | None = None
        self._poll_task: asyncio.Task | None = None

    async def initialize(self) -> None:
        """加载订阅数据并启动轮询任务。"""
        saved = await self.get_kv_data("subscriptions", {})
        if isinstance(saved, dict):
            self.subscriptions = {
                str(umo): {
                    "repos": dict(data.get("repos") or {}),
                    "users": dict(data.get("users") or {}),
                }
                for umo, data in saved.items()
                if isinstance(data, dict)
            }
        self._session = aiohttp.ClientSession()
        self._api = GithubApi(self._session, self.github_token, self.proxy)
        self._poll_task = asyncio.create_task(self._poll_loop())
        self.logger.info(
            "GitHub 订阅插件已启动：轮询间隔 %s 秒，%s 个会话，Token %s，代理 %s。",
            self.poll_interval,
            len(self.subscriptions),
            "已配置" if self.github_token else "未配置（60 次/小时）",
            self.proxy or "未配置",
        )

    async def terminate(self) -> None:
        """停用插件时取消轮询并释放会话。"""
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._session:
            await self._session.close()
            self._session = None
        self._api = None

    async def _save(self) -> None:
        """把订阅数据持久化到插件 KV 存储。"""
        try:
            await self.put_kv_data("subscriptions", self.subscriptions)
        except Exception as exc:
            self.logger.error("保存 GitHub 订阅数据失败: %s", exc)

    def _session_subs(self, event: AstrMessageEvent) -> dict[str, dict[str, Any]]:
        """取出当前会话的订阅容器（返回内部引用，可直接修改）。

        Args:
            event: 消息事件。

        Returns:
            包含 ``repos`` 与 ``users`` 两个字典的订阅容器。
        """
        return self.subscriptions.setdefault(
            event.unified_msg_origin,
            {"repos": {}, "users": {}},
        )

    async def _poll_loop(self) -> None:
        """周期轮询所有订阅。"""
        # 插件刚加载时先等一会，避免和其他初始化抢资源。
        await asyncio.sleep(15)
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error("GitHub 订阅轮询异常: %s", exc, exc_info=True)
            await asyncio.sleep(self._next_interval())

    def _next_interval(self) -> int:
        """根据剩余额度决定下一轮等待时间。

        Returns:
            需要等待的秒数。
        """
        if not self._api or self._api.rate_limit_remaining is None:
            return self.poll_interval
        if self._api.rate_limit_remaining > 50:
            return self.poll_interval
        # 额度吃紧时逐级放慢，最低仍保持一个轮询间隔。
        if self._api.rate_limit_remaining > 10:
            return self.poll_interval * 2
        if self._api.rate_limit_remaining > 0:
            return self.poll_interval * 5
        # 额度耗尽，等到窗口重置后再继续，最多等 15 分钟。
        wait = self._api.rate_limit_reset - int(time.time())
        return max(min(wait, 900), self.poll_interval)

    async def _poll_once(self) -> None:
        """遍历所有会话的订阅并检查更新。"""
        if not self._api:
            return
        # 同一仓库可能被多个会话订阅，先归并成一份检查清单，避免重复请求。
        # 必须保留订阅字典本身的引用，否则 seen_event_ids 的推进不会写回。
        repo_targets: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        user_targets: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        for umo, data in self.subscriptions.items():
            for key, sub in (data.get("repos") or {}).items():
                repo_targets.setdefault(key, []).append((umo, sub))
            for login, sub in (data.get("users") or {}).items():
                user_targets.setdefault(login, []).append((umo, sub))

        changed = False
        for repo, targets in repo_targets.items():
            try:
                events = await self._api.get(
                    f"/repos/{repo}/events",
                    {"per_page": 100},
                )
                if isinstance(events, list):
                    changed |= await self._dispatch(
                        await self._extract_events(events, targets),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error("检查仓库 %s 失败: %s", repo, exc)

        for login, targets in user_targets.items():
            try:
                events = await self._api.get(
                    f"/users/{login}/events/public",
                    {"per_page": 100},
                )
                if isinstance(events, list):
                    changed |= await self._dispatch(
                        await self._extract_events(events, targets),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error("检查开发者 %s 失败: %s", login, exc)

        if changed:
            await self._save()

    async def _extract_events(
        self,
        events: list[dict[str, Any]],
        targets: list[tuple[str, dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        """把原始事件流转换为待汇报的内容，并推进各订阅的基线。

        Args:
            events: GitHub 事件列表。
            targets: (会话标识, 订阅字典) 列表。

        Returns:
            待汇报项列表，每项额外带上目标会话 umo。
        """
        # 事件接口按时间倒序返回，反转后从旧到新处理，汇报顺序才符合时间线。
        ordered = list(reversed(events))
        # 先为每个订阅算出待处理事件。dict.fromkeys 去重且保留顺序，
        # 用 set 会丢顺序，截断时可能保留旧 id 导致事件重放。
        pending: list[tuple[str, dict[str, Any], list[str], list[dict[str, Any]]]] = []
        for umo, target in targets:
            seen_order = list(
                dict.fromkeys(str(item) for item in target.get("seen_event_ids") or []),
            )
            seen = set(seen_order)
            fresh = [
                event
                for event in ordered
                if (event_id := str(event.get("id") or "")) and event_id not in seen
            ]
            pending.append((umo, target, seen_order, fresh))

        # 基线阶段只登记事件 id，不解析内容，也就不会产生额外请求。
        active: list[tuple[str, dict[str, Any], list[str], list[dict[str, Any]]]] = []
        for umo, target, seen_order, fresh in pending:
            if target.get("baseline_done"):
                active.append((umo, target, seen_order, fresh))
                continue
            target["seen_event_ids"] = (
                seen_order + [str(event.get("id") or "") for event in fresh]
            )[-300:]
            target["baseline_done"] = True

        if not active:
            return []

        # 优先使用 PushEvent 自带的 commits；只有被 GitHub 截断（数组为空）时
        # 才需要 compare 补全。同一分支多条 push 合并成一次 compare 请求，
        # 否则每条 push 各发一次请求，订阅一多就会打爆限额。
        groups: dict[tuple[str, str], dict[str, str]] = {}
        for _umo, _target, _seen, fresh in active:
            for event in fresh:
                if event.get("type") != "PushEvent":
                    continue
                payload = event.get("payload") or {}
                ref = payload.get("ref") or ""
                # 自带 commits 就不必再发请求。
                if not ref.startswith(PUSH_REF_PREFIX) or payload.get("commits"):
                    continue
                key = ((event.get("repo") or {}).get("name") or "", ref)
                group = groups.setdefault(key, {"before": "", "head": ""})
                before = payload.get("before") or ""
                # 最早那条事件（头部）的 before 才是整段区间的起点；
                # 全 0 的 before 表示新建分支，没有可比较的基线。
                if not group["before"] and before and set(before) != {"0"}:
                    group["before"] = before
                head = payload.get("head") or ""
                if head:
                    group["head"] = head

        # 只对需要补全 commit 的分支发请求，拿不到的记入失败集合，下轮重试。
        resolved: dict[tuple[str, str], list[dict[str, str]]] = {}
        failed: set[tuple[str, str]] = set()
        for (repo, ref), group in groups.items():
            before, head = group["before"], group["head"]
            if not head or not before:
                # 新建分支没有比较基线，靠 payload 自带 commit 或直接跳过。
                continue
            data = await self._api_get_compare(repo, before, head)
            if data is None:
                failed.add((repo, ref))
                continue
            resolved[(repo, ref)] = data

        results: list[dict[str, Any]] = []
        for umo, target, seen_order, fresh in active:
            # 同一会话同一分支的多条 push 只有一份 commit 明细，合并成一条汇报。
            branch_items: dict[tuple[str, str], dict[str, Any]] = {}
            ok_ids: list[str] = []
            for event in fresh:
                event_id = str(event.get("id") or "")
                event_type = event.get("type")
                repo = (event.get("repo") or {}).get("name") or ""

                if event_type == "PushEvent":
                    payload = event.get("payload") or {}
                    ref = payload.get("ref") or ""
                    if not ref.startswith(PUSH_REF_PREFIX):
                        ok_ids.append(event_id)
                        continue
                    key = (repo, ref)
                    if key in failed:
                        # 明细没取到，先不登记，下一轮重新尝试。
                        continue
                    commits = resolved.get(key) or self._inline_commits(payload)
                    if not commits:
                        ok_ids.append(event_id)
                        continue
                    merged = branch_items.setdefault(
                        key,
                        {
                            "kind": "commit",
                            "repo": repo,
                            "branch": ref[len(PUSH_REF_PREFIX) :],
                            "actor": (event.get("actor") or {}).get("login") or "",
                            "commits": [],
                        },
                    )
                    for commit in commits:
                        if commit not in merged["commits"]:
                            merged["commits"].append(commit)
                    ok_ids.append(event_id)
                elif event_type == "ReleaseEvent":
                    item = self._build_release(event)
                    if item:
                        results.append({**item, "umo": umo})
                    ok_ids.append(event_id)
                else:
                    ok_ids.append(event_id)

            for item in branch_items.values():
                # 合并后可能超过上限，保留最新的若干条。
                if len(item["commits"]) > self.commit_detail_limit:
                    item["commits"] = item["commits"][-self.commit_detail_limit :]
                results.append({**item, "umo": umo})
            target["seen_event_ids"] = (seen_order + ok_ids)[-300:]
        return results

    async def _api_get_compare(
        self,
        repo: str,
        before: str,
        head: str,
    ) -> list[dict[str, str]] | None:
        """请求 compare 接口并归一化 commit 列表。

        Args:
            repo: 仓库全名。
            before: 区间起点 commit。
            head: 区间终点 commit。

        Returns:
            commit 列表；请求失败返回 None（用于区分「失败」和「没有提交」）。
        """
        assert self._api
        if before == head:
            return []
        data = await self._api.get(f"/repos/{repo}/compare/{before}...{head}")
        if data is None:
            return None
        commits: list[dict[str, str]] = []
        for commit in data.get("commits") or []:
            info = commit.get("commit") or {}
            commits.append(
                {
                    "sha": (commit.get("sha") or "")[:7],
                    # 只保留标题行，正文对汇报没有额外价值。
                    "message": (info.get("message") or "").split("\n", 1)[0].strip(),
                    "author": (info.get("author") or {}).get("name") or "",
                },
            )
        return commits

    def _inline_commits(self, payload: dict[str, Any]) -> list[dict[str, str]]:
        """取出 PushEvent 自带的 commit 列表。

        高活跃仓库上 GitHub 会把这个数组截断为空，此时需要 compare 接口补全。

        Args:
            payload: PushEvent 的 payload。

        Returns:
            归一化后的 commit 列表。
        """
        commits: list[dict[str, str]] = []
        for commit in payload.get("commits") or []:
            # events 里的 commit 结构与 compare 接口不同，统一成同一格式。
            commits.append(
                {
                    "sha": (commit.get("sha") or "")[:7],
                    "message": (commit.get("message") or "").split("\n", 1)[0].strip(),
                    "author": (commit.get("author") or {}).get("name") or "",
                },
            )
        return commits

    def _build_release(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """把 ReleaseEvent 转换为待汇报项。

        Args:
            event: ReleaseEvent 事件。

        Returns:
            待汇报项；草稿、被过滤或数据缺失时返回 None。
        """
        release = (event.get("payload") or {}).get("release") or {}
        if release.get("draft"):
            return None
        tag = release.get("tag_name") or ""
        name = release.get("name") or tag
        if self._should_skip_tag(f"{tag} {name}"):
            return None
        return {
            "kind": "release",
            "repo": (event.get("repo") or {}).get("name") or "",
            "tag": tag,
            "name": name,
            "url": release.get("html_url") or "",
            # 实测 release 正文可能上万字符，交给模型前必须截断。
            "body": _trim(release.get("body") or "", self.max_changelog_chars),
            "prerelease": bool(release.get("prerelease")),
            "actor": (event.get("actor") or {}).get("login") or "",
        }

    def _should_skip_tag(self, text: str) -> bool:
        """判断版本标签是否命中跳过规则。

        Args:
            text: tag 与标题拼成的文本。

        Returns:
            命中任一关键词时返回 True。
        """
        if not self.skip_tag_keywords:
            return False
        lowered = text.lower()
        return any(keyword in lowered for keyword in self.skip_tag_keywords)

    async def _dispatch(self, items: list[dict[str, Any]]) -> bool:
        """把同一会话、同一仓库的多条更新合并成一次汇报并推送。

        Args:
            items: 待汇报项列表。

        Returns:
            是否处理了任何内容。
        """
        if not items:
            return False
        # 按 (会话, 仓库) 归并，避免一个仓库一次推好几条消息。
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for item in items:
            grouped.setdefault((item["umo"], item["repo"]), []).append(item)
        for (umo, repo), batch in grouped.items():
            try:
                await self._report(umo, repo, batch)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error("推送 GitHub 更新到 %s 失败: %s", umo, exc)
        return True

    async def _report(
        self,
        umo: str,
        repo: str,
        batch: list[dict[str, Any]],
    ) -> None:
        """生成汇报文本并发送到会话，失败时降级为格式化文本。

        Args:
            umo: 目标会话。
            repo: 仓库全名。
            batch: 同一仓库的多条更新。
        """
        text = await self._generate_report(umo, repo, batch)
        if not text:
            text = self._format_fallback(repo, batch)
        await self.context.send_message(umo, MessageChain([Plain(text)]))

    async def _generate_report(
        self,
        umo: str,
        repo: str,
        batch: list[dict[str, Any]],
    ) -> str:
        """调用模型生成汇报文本。

        Args:
            umo: 会话标识，用于取会话偏好的模型。
            repo: 仓库全名。
            batch: 同一仓库的多条更新。

        Returns:
            模型生成的文本；失败或为空时返回空字符串。
        """
        try:
            provider = None
            if self.report_provider_id:
                provider = self.context.get_provider_by_id(self.report_provider_id)
            if not provider:
                provider = await self.context.get_using_provider_async(umo)
            if not provider:
                return ""
            # 串行 + 超时：单个仓库汇报失败不会拖垮整轮轮询。
            resp = await asyncio.wait_for(
                provider.text_chat(
                    prompt=self._build_prompt(repo, batch),
                    system_prompt=REPORT_SYSTEM_PROMPT,
                ),
                timeout=self.report_timeout,
            )
            return (getattr(resp, "completion_text", "") or "").strip()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.warning("生成 GitHub 汇报失败，降级为文本推送: %s", exc)
            return ""

    def _build_prompt(self, repo: str, batch: list[dict[str, Any]]) -> str:
        """把结构化更新整理成给模型的提示词。

        Args:
            repo: 仓库全名。
            batch: 同一仓库的多条更新。

        Returns:
            提示词文本。
        """
        lines = [f"仓库：{repo}", ""]
        for item in batch:
            if item["kind"] == "commit":
                lines.append(
                    f"【新提交】分支 {item['branch']}，提交者 "
                    f"{item['actor'] or '未知'}，共 {len(item['commits'])} 条 commit：",
                )
                for commit in item["commits"]:
                    lines.append(f"- {commit['sha']} {commit['message']}")
                lines.append("")
            elif item["kind"] == "release":
                kind = "预发布版本" if item["prerelease"] else "正式版本"
                lines.append(
                    f"【新 Release】{kind} {item['name']}（tag {item['tag']}）"
                )
                if item["url"]:
                    lines.append(f"链接：{item['url']}")
                if item["body"]:
                    lines.append("更新日志：")
                    lines.append(item["body"])
                else:
                    lines.append("（该 Release 没有提供更新日志）")
                lines.append("")
        lines.append("请根据以上信息向用户汇报。")
        return "\n".join(lines)

    def _format_fallback(self, repo: str, batch: list[dict[str, Any]]) -> str:
        """模型不可用时使用的格式化文本。

        Args:
            repo: 仓库全名。
            batch: 同一仓库的多条更新。

        Returns:
            可直接发送的文本。
        """
        lines = [f"📦 {repo} 有新动态："]
        for item in batch:
            if item["kind"] == "commit":
                lines.append(
                    f"- 分支 {item['branch']} 新增 {len(item['commits'])} 个提交：",
                )
                for commit in item["commits"][:5]:
                    lines.append(f"  · {commit['sha']} {commit['message']}")
            elif item["kind"] == "release":
                label = "预发布" if item["prerelease"] else "正式版"
                lines.append(f"- 发布{label} {item['name']}（tag {item['tag']}）")
                if item["body"]:
                    lines.append(f"  更新日志：{_trim(item['body'], 300)}")
                if item["url"]:
                    lines.append(f"  {item['url']}")
        return "\n".join(lines)

    @filter.llm_tool(name="github_search_repos")
    async def search_repos(self, event: AstrMessageEvent, keyword: str) -> str:
        """按关键词搜索 GitHub 仓库，返回仓库名、star 数、语言和简介。

        Args:
            keyword(string): 搜索关键词，例如 astrbot、图片生成、python llm
        """
        if not self._api:
            return "插件尚未初始化完成，请稍后再试。"
        keyword = keyword.strip()
        if not keyword:
            return "请提供要搜索的关键词。"
        data = await self._api.get(
            "/search/repositories",
            {
                "q": keyword,
                "sort": "stars",
                "order": "desc",
                "per_page": self.max_search_results,
            },
        )
        items = (data or {}).get("items") or []
        if not items:
            return (
                f"没有搜索到与「{keyword}」相关的仓库，"
                "可能是关键词太窄，或者触发了 GitHub 搜索接口限流。"
            )

        lines = [f"搜索「{keyword}」的结果："]
        for index, item in enumerate(items, start=1):
            desc = (item.get("description") or "").strip().replace("\n", " ")
            if len(desc) > 80:
                desc = desc[:80] + "…"
            lines.append(
                f"{index}. {item['full_name']}"
                f"（⭐ {item.get('stargazers_count', 0)}，"
                f"{item.get('language') or '未知语言'}）：{desc or '无简介'}",
            )
        return "\n".join(lines)

    @filter.llm_tool(name="github_subscribe_repo")
    async def subscribe_repo(self, event: AstrMessageEvent, repo: str) -> str:
        """订阅一个 GitHub 仓库，有新 commit 或发布 Release 时在当前会话提醒。

        Args:
            repo(string): 仓库全名，格式为 owner/repo，例如 AstrBotDevs/AstrBot
        """
        if not self._api:
            return "插件尚未初始化完成，请稍后再试。"
        if not event.is_admin():
            return "只有 AstrBot 管理员才能创建订阅，请联系管理员操作。"
        repo = repo.strip().strip("/")
        if repo.count("/") != 1:
            return "仓库名格式不对，请用 owner/repo 的形式，例如 AstrBotDevs/AstrBot。"
        key = repo.lower()
        data = self._session_subs(event)
        if key in data["repos"]:
            return f"当前会话已经订阅了仓库 {repo}，无需重复订阅。"

        info = await self._api.get(f"/repos/{repo}")
        if not isinstance(info, dict) or not info.get("full_name"):
            return (
                f"没有找到仓库 {repo}，请确认仓库名是否正确"
                "（私有仓库或触发限流时也会失败）。"
            )

        data["repos"][key] = {
            "full_name": info["full_name"],
            "default_branch": info.get("default_branch") or "",
            "subscribed_at": int(time.time()),
            # 第一轮轮询只建基线，避免把历史事件当成新内容推出去。
            "baseline_done": False,
            "seen_event_ids": [],
        }
        await self._save()
        return (
            f"订阅成功：已订阅仓库 {info['full_name']}"
            f"（⭐ {info.get('stargazers_count', 0)}），"
            f"当前会话共 {len(data['repos'])} 个仓库订阅、"
            f"{len(data['users'])} 个开发者订阅。"
            "之后该仓库任意分支有新 commit 或发布 Release 时都会在本会话提醒。"
        )

    @filter.llm_tool(name="github_subscribe_user")
    async def subscribe_user(self, event: AstrMessageEvent, username: str) -> str:
        """订阅一个 GitHub 开发者，他向任意仓库提交 commit 或发版时都会提醒。

        Args:
            username(string): GitHub 用户名，例如 torvalds
        """
        if not self._api:
            return "插件尚未初始化完成，请稍后再试。"
        if not event.is_admin():
            return "只有 AstrBot 管理员才能创建订阅，请联系管理员操作。"
        username = username.strip().lstrip("@").strip("/")
        if not username:
            return "请提供要订阅的 GitHub 用户名。"
        key = username.lower()
        data = self._session_subs(event)
        if key in data["users"]:
            return f"当前会话已经订阅了开发者 {username}，无需重复订阅。"

        info = await self._api.get(f"/users/{username}")
        if not isinstance(info, dict) or not info.get("login"):
            return (
                f"没有找到用户 {username}，请确认用户名是否正确"
                "（组织账号或触发限流时也会失败）。"
            )

        data["users"][key] = {
            "login": info["login"],
            "subscribed_at": int(time.time()),
            "baseline_done": False,
            "seen_event_ids": [],
        }
        await self._save()
        return (
            f"订阅成功：已订阅开发者 {info['login']}"
            f"（{info.get('name') or '未填写昵称'}），"
            f"当前会话共 {len(data['users'])} 个开发者订阅、"
            f"{len(data['repos'])} 个仓库订阅。"
            "之后他向任意仓库提交 commit 或发布 Release 时都会在本会话提醒。"
        )

    @filter.llm_tool(name="github_unsubscribe")
    async def unsubscribe(self, event: AstrMessageEvent, target: str) -> str:
        """取消订阅某个仓库或开发者。

        Args:
            target(string): 要取消订阅的仓库全名（owner/repo）或 GitHub 用户名
        """
        if not event.is_admin():
            return "只有 AstrBot 管理员才能取消订阅，请联系管理员操作。"
        target = target.strip().lstrip("@").strip("/")
        if not target:
            return "请提供要取消订阅的仓库名或用户名。"
        key = target.lower()
        data = self._session_subs(event)
        if key in data["repos"]:
            full_name = data["repos"].pop(key).get("full_name") or target
            await self._save()
            return (
                f"已取消订阅仓库 {full_name}，"
                f"当前会话还剩 {len(data['repos'])} 个仓库订阅。"
            )
        if key in data["users"]:
            login = data["users"].pop(key).get("login") or target
            await self._save()
            return (
                f"已取消订阅开发者 {login}，"
                f"当前会话还剩 {len(data['users'])} 个开发者订阅。"
            )
        return f"当前会话没有订阅 {target}。"

    @filter.llm_tool(name="github_list_subscriptions")
    async def list_subscriptions(self, event: AstrMessageEvent) -> str:
        """查看当前会话订阅的 GitHub 仓库和开发者列表。"""
        data = self.subscriptions.get(event.unified_msg_origin) or {}
        repos = data.get("repos") or {}
        users = data.get("users") or {}
        if not repos and not users:
            return "当前会话还没有订阅任何 GitHub 仓库或开发者。"

        lines = [
            f"当前会话共订阅了 {len(repos)} 个仓库、{len(users)} 个开发者：",
        ]
        for sub in repos.values():
            added = int(sub.get("subscribed_at") or 0)
            when = (
                time.strftime("%Y-%m-%d %H:%M", time.localtime(added))
                if added
                else "未知"
            )
            lines.append(f"- 仓库 {sub.get('full_name')}（订阅于 {when}）")
        for sub in users.values():
            added = int(sub.get("subscribed_at") or 0)
            when = (
                time.strftime("%Y-%m-%d %H:%M", time.localtime(added))
                if added
                else "未知"
            )
            lines.append(f"- 开发者 {sub.get('login')}（订阅于 {when}）")
        return "\n".join(lines)

    @filter.command_group("ghsub")
    def ghsub(self):
        """GitHub 订阅管理。"""

    @ghsub.command("list")
    async def list_subscriptions_cmd(self, event: AstrMessageEvent):
        """查看当前会话的 GitHub 订阅列表。"""
        data = self.subscriptions.get(event.unified_msg_origin) or {}
        repos = data.get("repos") or {}
        users = data.get("users") or {}
        if not repos and not users:
            yield event.plain_result(
                "当前会话还没有订阅任何 GitHub 仓库或开发者。可以让我搜索并订阅。",
            )
            return
        lines = ["当前会话的 GitHub 订阅："]
        for sub in repos.values():
            lines.append(f"- 仓库 {sub.get('full_name')}")
        for sub in users.values():
            lines.append(f"- 开发者 {sub.get('login')}")
        yield event.plain_result("\n".join(lines))

    @ghsub.command("check")
    async def check_now(self, event: AstrMessageEvent):
        """立刻检查一次所有订阅，不等待下一轮轮询。"""
        if not event.is_admin():
            yield event.plain_result("只有 AstrBot 管理员才能手动触发检查。")
            return
        try:
            await self._poll_once()
        except Exception as exc:
            yield event.plain_result(f"检查失败：{exc}")
            return
        yield event.plain_result("已检查全部订阅。")
