"""订阅核心逻辑的离线单元测试。

只测试纯逻辑（事件解析、去重、基线、去重合并、降级文本），
不依赖 astrbot 运行时，也不发起真实网络请求。
"""

from __future__ import annotations

import asyncio
import importlib.util
import sys
import types
from pathlib import Path
from typing import Any

PLUGIN_DIR = Path(__file__).resolve().parent.parent


def _load_module() -> Any:
    """在缺少 astrbot 运行时的情况下加载 main.py。

    Returns:
        已加载的插件模块对象。
    """
    # 构造最小可用的 astrbot 桩，只为让模块级 import 通过。
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    event_mod = types.ModuleType("astrbot.api.event")
    components_mod = types.ModuleType("astrbot.api.message_components")
    star_mod = types.ModuleType("astrbot.api.star")

    class _Commandable:
        """占位指令组，支持再挂子指令。"""

        def command(self, _name: str):
            def _wrap(func: Any) -> Any:
                return func

            return _wrap

    class _Filter:
        """占位 filter，装饰器原样返回函数。"""

        @staticmethod
        def llm_tool(**_kwargs: Any):
            def _wrap(func: Any) -> Any:
                return func

            return _wrap

        @staticmethod
        def command_group(_name: str):
            def _wrap(func: Any) -> Any:
                # 真实实现会返回可继续挂子指令的对象。
                return _Commandable()

            return _wrap

    class _AstrMessageEvent:
        """占位消息事件。"""

        unified_msg_origin = "test:session"
        role = "admin"

        def is_admin(self) -> bool:
            return True

        def plain_result(self, text: str) -> str:
            return text

    class _MessageChain:
        """占位消息链。"""

        def __init__(self, chain: list[Any]) -> None:
            self.chain = chain

    class _Plain:
        """占位纯文本组件。"""

        def __init__(self, text: str) -> None:
            self.text = text

    class _Context:
        """占位上下文。"""

    class _Star:
        """占位插件基类。"""

        def __init__(self, context: Any, config: dict | None = None) -> None:
            self.context = context
            self.logger = types.SimpleNamespace(
                info=lambda *a, **k: None,
                error=lambda *a, **k: None,
                warning=lambda *a, **k: None,
            )

    event_mod.AstrMessageEvent = _AstrMessageEvent
    event_mod.MessageChain = _MessageChain
    event_mod.filter = _Filter()
    components_mod.Plain = _Plain
    star_mod.Context = _Context
    star_mod.Star = _Star

    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.event": event_mod,
            "astrbot.api.message_components": components_mod,
            "astrbot.api.star": star_mod,
        },
    )
    spec = importlib.util.spec_from_file_location(
        "ghsub_main",
        PLUGIN_DIR / "main.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


class _FakeApi:
    """按路径返回预置数据的假 API。"""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[str] = []
        self.rate_limit_remaining = 100
        self.rate_limit_reset = 0

    async def get(self, path: str, params: dict | None = None) -> Any:
        self.calls.append(path)
        for key, value in self.responses.items():
            if path.startswith(key):
                return value
        return None


def _plugin(**config: Any):
    """构造一个带假 API 的插件实例。

    Args:
        **config: 覆盖默认配置。

    Returns:
        插件实例。
    """
    plugin = mod.GithubSubscribePlugin(context=types.SimpleNamespace(), config=config)
    return plugin


def _push_event(event_id: str, before: str, head: str, ref: str = "refs/heads/main"):
    """构造一条 PushEvent。

    Args:
        event_id: 事件 id。
        before: push 前的 commit。
        head: push 后的 commit。
        ref: git ref。

    Returns:
        事件字典。
    """
    return {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"login": "alice"},
        "repo": {"name": "acme/widget"},
        "payload": {
            "ref": ref,
            "before": before,
            "head": head,
            "commits": [],
        },
    }


def _release_event(event_id: str, tag: str, name: str = "", body: str = ""):
    """构造一条 ReleaseEvent。

    Args:
        event_id: 事件 id。
        tag: tag 名。
        name: release 标题。
        body: 更新日志。

    Returns:
        事件字典。
    """
    return {
        "id": event_id,
        "type": "ReleaseEvent",
        "actor": {"login": "bob"},
        "repo": {"name": "acme/widget"},
        "payload": {
            "action": "published",
            "release": {
                "tag_name": tag,
                "name": name or tag,
                "body": body,
                "draft": False,
                "prerelease": False,
                "html_url": f"https://github.com/acme/widget/releases/tag/{tag}",
            },
        },
    }


def test_baseline_does_not_report_history() -> None:
    """第一次轮询只建立基线，不汇报历史事件。"""
    plugin = _plugin()
    plugin._api = _FakeApi(
        {
            "/repos/acme/widget/compare": {
                "commits": [
                    {"sha": "a" * 40, "commit": {"message": "old", "author": {"name": "x"}}},
                ],
            },
        },
    )
    target = {"baseline_done": False, "seen_event_ids": []}
    items = asyncio.run(
        plugin._extract_events(
            [_push_event("1", "f" * 40, "a" * 40)],
            [("umo", target)],
        ),
    )
    assert items == []
    assert target["baseline_done"] is True
    assert target["seen_event_ids"] == ["1"]
    # 建基线阶段不该发起 compare 请求。
    assert plugin._api.calls == []


def test_second_poll_reports_new_pushes_only() -> None:
    """建立基线后只汇报新增事件，已处理的事件不重复。"""
    plugin = _plugin()
    plugin._api = _FakeApi(
        {
            "/repos/acme/widget/compare": {
                "commits": [
                    {"sha": "b" * 40, "commit": {"message": "fix: x\n\nbody", "author": {"name": "alice"}}},
                ],
            },
        },
    )
    target = {"baseline_done": True, "seen_event_ids": ["1"]}
    events = [
        _push_event("2", "a" * 40, "b" * 40),
        _push_event("1", "f" * 40, "a" * 40),
    ]
    items = asyncio.run(plugin._extract_events(events, [("umo", target)]))
    assert len(items) == 1
    assert items[0]["kind"] == "commit"
    assert items[0]["umo"] == "umo"
    assert items[0]["branch"] == "main"
    # compare 拿到的 message 只保留标题行。
    assert items[0]["commits"][0]["message"] == "fix: x"
    assert target["seen_event_ids"] == ["1", "2"]


def test_shared_repo_across_sessions_builds_item_once() -> None:
    """同一事件被多个会话订阅时，只解析一次。"""
    plugin = _plugin()
    plugin._api = _FakeApi(
        {
            "/repos/acme/widget/compare": {
                "commits": [{"sha": "c" * 40, "commit": {"message": "feat: y"}}],
            },
        },
    )
    t1 = {"baseline_done": True, "seen_event_ids": []}
    t2 = {"baseline_done": True, "seen_event_ids": []}
    items = asyncio.run(
        plugin._extract_events(
            [_push_event("5", "a" * 40, "c" * 40)],
            [("s1", t1), ("s2", t2)],
        ),
    )
    assert {item["umo"] for item in items} == {"s1", "s2"}
    assert len(plugin._api.calls) == 1


def test_release_respects_tag_filter_and_changelog_limit() -> None:
    """Release 会按标签关键词过滤，并按配置截断更新日志。"""
    plugin = _plugin(skip_tag_pattern="alpha, rc", max_changelog_chars=50)
    target = {"baseline_done": True, "seen_event_ids": []}
    events = [
        _release_event("1", "v1.0.0-alpha.1", body="x" * 200),
        _release_event("2", "v2.0.0", body="y" * 200),
    ]
    items = asyncio.run(plugin._extract_events(events, [("umo", target)]))
    assert len(items) == 1
    assert items[0]["tag"] == "v2.0.0"
    assert "已截断" in items[0]["body"]
    assert items[0]["body"].startswith("y" * 50)


def test_draft_release_is_ignored() -> None:
    """草稿 Release 不推送。"""
    plugin = _plugin()
    event = _release_event("1", "v9.9.9")
    event["payload"]["release"]["draft"] = True
    target = {"baseline_done": True, "seen_event_ids": []}
    items = asyncio.run(plugin._extract_events([event], [("umo", target)]))
    assert items == []


def test_non_branch_push_is_ignored() -> None:
    """tag 等非分支 ref 的 push 不计入 commit 汇报。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})
    target = {"baseline_done": True, "seen_event_ids": []}
    event = _push_event("1", "a" * 40, "b" * 40, ref="refs/tags/v1")
    items = asyncio.run(plugin._extract_events([event], [("umo", target)]))
    assert items == []


def test_inline_commits_used_without_compare() -> None:
    """PushEvent 自带 commits 时不再额外请求 compare。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})
    target = {"baseline_done": True, "seen_event_ids": []}
    event = _push_event("1", "a" * 40, "b" * 40)
    event["payload"]["commits"] = [
        {"sha": "b" * 40, "message": "docs: z", "author": {"name": "carol"}},
    ]
    items = asyncio.run(plugin._extract_events([event], [("umo", target)]))
    assert len(items) == 1
    assert items[0]["commits"][0]["message"] == "docs: z"
    assert plugin._api.calls == []


def test_new_branch_with_zero_before_is_skipped() -> None:
    """新建分支（before 全 0）没有可比较基线，直接跳过。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})
    target = {"baseline_done": True, "seen_event_ids": []}
    event = _push_event("1", "0" * 40, "b" * 40)
    items = asyncio.run(plugin._extract_events([event], [("umo", target)]))
    assert items == []


def test_commit_detail_limit() -> None:
    """超出上限时只保留最新的若干条 commit。"""
    plugin = _plugin(commit_detail_limit=2)
    plugin._api = _FakeApi({})
    target = {"baseline_done": True, "seen_event_ids": []}
    event = _push_event("1", "a" * 40, "b" * 40)
    event["payload"]["commits"] = [
        {"sha": str(i) * 7, "message": f"c{i}", "author": {"name": "x"}}
        for i in range(5)
    ]
    items = asyncio.run(plugin._extract_events([event], [("umo", target)]))
    assert [c["message"] for c in items[0]["commits"]] == ["c3", "c4"]


def test_seen_ids_are_capped() -> None:
    """seen_event_ids 只保留最近 300 个，避免无限膨胀。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})
    target = {"baseline_done": True, "seen_event_ids": [str(i) for i in range(400)]}
    asyncio.run(plugin._extract_events([], [("umo", target)]))
    assert len(target["seen_event_ids"]) == 300
    assert target["seen_event_ids"][-1] == "399"


def test_dispatch_groups_by_session_and_repo() -> None:
    """同一会话同一仓库的多条更新合并成一次推送。"""
    plugin = _plugin()
    sent: list[tuple[str, str]] = []

    async def _fake_report(umo: str, repo: str, batch: list[dict]) -> None:
        sent.append((umo, repo))
        assert len(batch) == 2

    plugin._report = _fake_report
    items = [
        {"umo": "s1", "repo": "acme/widget", "kind": "commit"},
        {"umo": "s1", "repo": "acme/widget", "kind": "release"},
        {"umo": "s2", "repo": "acme/widget", "kind": "commit"},
    ]
    asyncio.run(plugin._dispatch(items))
    assert sent == [("s1", "acme/widget"), ("s2", "acme/widget")]


def test_fallback_text_contains_version_and_link() -> None:
    """降级文本包含版本号、更新日志与链接。"""
    plugin = _plugin()
    batch = [
        {
            "kind": "release",
            "repo": "acme/widget",
            "tag": "v2.0.0",
            "name": "v2.0.0",
            "body": "新增功能",
            "url": "https://example.com/rel",
            "prerelease": False,
        },
    ]
    text = plugin._format_fallback("acme/widget", batch)
    assert "v2.0.0" in text
    assert "新增功能" in text
    assert "https://example.com/rel" in text


def test_prompt_includes_commit_names_and_changelog() -> None:
    """提示词包含 commit 名、版本号与更新日志。"""
    plugin = _plugin()
    batch = [
        {
            "kind": "commit",
            "repo": "acme/widget",
            "branch": "main",
            "actor": "alice",
            "commits": [{"sha": "abc1234", "message": "fix: bug", "author": "alice"}],
        },
        {
            "kind": "release",
            "repo": "acme/widget",
            "tag": "v1.2.3",
            "name": "v1.2.3",
            "body": "修复了一些问题",
            "url": "https://example.com/rel",
            "prerelease": False,
        },
    ]
    prompt = plugin._build_prompt("acme/widget", batch)
    assert "abc1234" in prompt
    assert "fix: bug" in prompt
    assert "v1.2.3" in prompt
    assert "修复了一些问题" in prompt


def test_trim_behaviour() -> None:
    """_trim 在阈值内原样返回，超长时截断并提示。"""
    assert mod._trim("abc", 10) == "abc"
    assert mod._trim("abcdef", 3).startswith("abc")
    assert "已截断" in mod._trim("abcdef", 3)
    assert mod._trim("abcdef", 0) == ""


def test_next_interval_backs_off_when_exhausted() -> None:
    """额度耗尽时下一轮等待时间会显著拉长。"""
    plugin = _plugin(poll_interval=120)
    plugin._api = _FakeApi({})
    plugin._api.rate_limit_remaining = 0
    plugin._api.rate_limit_reset = int(__import__("time").time()) + 600
    assert plugin._next_interval() >= 120
    plugin._api.rate_limit_remaining = 100
    assert plugin._next_interval() == 120


def test_rate_limit_records_minimum() -> None:
    """限流记录取历史最小值，避免被搜索接口的大额度冲掉。"""
    plugin = _plugin()
    api = mod.GithubApi(session=None)
    api._record_rate_limit({"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": "1"})
    assert api.rate_limit_remaining == 100
    api._record_rate_limit({"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": "2"})
    assert api.rate_limit_remaining == 3
    api._record_rate_limit({"X-RateLimit-Remaining": "50", "X-RateLimit-Reset": "3"})
    assert api.rate_limit_remaining == 3
    assert api.rate_limit_reset == 3


def test_multiple_pushes_share_one_compare() -> None:
    """同一分支的多条 push 只发一次 compare，且合并成一条汇报。"""
    plugin = _plugin()
    plugin._api = _FakeApi(
        {
            "/repos/acme/widget/compare": {
                "commits": [
                    {"sha": "b" * 40, "commit": {"message": "feat: one"}},
                    {"sha": "c" * 40, "commit": {"message": "feat: two"}},
                ],
            },
        },
    )
    target = {"baseline_done": True, "seen_event_ids": []}
    events = [
        _push_event("2", "b" * 40, "c" * 40),
        _push_event("1", "a" * 40, "b" * 40),
    ]
    items = asyncio.run(plugin._extract_events(events, [("umo", target)]))
    assert len(plugin._api.calls) == 1
    assert len(items) == 1
    assert items[0]["branch"] == "main"
    assert [c["message"] for c in items[0]["commits"]] == ["feat: one", "feat: two"]
    assert target["seen_event_ids"] == ["1", "2"]


def test_compare_failure_keeps_event_for_retry() -> None:
    """compare 取不到明细时不登记事件 id，等下一轮重试，避免丢消息。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})  # 任何请求都返回 None，模拟失败
    target = {"baseline_done": True, "seen_event_ids": []}
    items = asyncio.run(
        plugin._extract_events([_push_event("1", "a" * 40, "b" * 40)], [("umo", target)]),
    )
    assert items == []
    assert target["seen_event_ids"] == []


def test_baseline_skips_compare_requests() -> None:
    """建基线阶段不应该发起任何 compare 请求。"""
    plugin = _plugin()
    plugin._api = _FakeApi({})
    target = {"baseline_done": False, "seen_event_ids": []}
    events = [_push_event("2", "b" * 40, "c" * 40), _push_event("1", "a" * 40, "b" * 40)]
    items = asyncio.run(plugin._extract_events(events, [("umo", target)]))
    assert items == []
    assert plugin._api.calls == []
    assert target["seen_event_ids"] == ["1", "2"]
