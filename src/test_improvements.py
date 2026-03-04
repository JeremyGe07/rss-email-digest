"""Tests for feed robustness and translation helpers."""
from types import SimpleNamespace

import pytest

from src import feed_parser
from src.translator import contains_cjk


class _FakeResponse:
    status = 200
    headers = {"Content-Type": "application/rss+xml"}
    url = "https://example.com/rss"

    async def read(self):
        return b"<rss></rss>"

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, *args, **kwargs):
        return _FakeResponse()


@pytest.mark.asyncio
async def test_bozo_feed_with_entries_is_not_error(monkeypatch):
    """Malformed feeds with entries should still be processed."""

    def fake_parse(_content):
        entry = SimpleNamespace(
            title="Nvidia launches new GPU",
            link="https://example.com/post",
            summary="AI chip update",
            published_parsed=(2026, 1, 1, 0, 0, 0, 0, 0, 0),
            updated_parsed=None,
        )
        return SimpleNamespace(
            bozo=True,
            bozo_exception=Exception("broken xml"),
            entries=[entry],
            feed={"link": "https://example.com"},
        )

    monkeypatch.setattr(feed_parser, "is_from_yesterday", lambda _: True)
    monkeypatch.setattr(feed_parser.aiohttp, "ClientSession", _FakeSession)
    monkeypatch.setattr(feed_parser.feedparser, "parse", fake_parse)

    result = await feed_parser.fetch_feed("Demo", "https://example.com/rss")

    assert result["status"] == "success"
    assert len(result["posts"]) == 1


@pytest.mark.asyncio
async def test_http_error_returns_error_status(monkeypatch):
    """HTTP status >= 400 should be reported clearly."""

    class _ErrorResponse(_FakeResponse):
        status = 502

    class _ErrorSession(_FakeSession):
        def get(self, *args, **kwargs):
            return _ErrorResponse()

    monkeypatch.setattr(feed_parser.aiohttp, "ClientSession", _ErrorSession)

    result = await feed_parser.fetch_feed("Demo", "https://example.com/rss")

    assert result["status"] == "error"
    assert result["error_message"] == "HTTP 502"


def test_contains_cjk_detects_chinese_text():
    assert contains_cjk("英伟达发布新芯片")
    assert not contains_cjk("Nvidia launches new chip")


@pytest.mark.asyncio
async def test_missing_date_fallback_keeps_recent_no_date_posts(monkeypatch):
    """When most entries have no date, fallback should inspect latest N no-date items."""

    def fake_parse(_content):
        entries = [
            SimpleNamespace(title="HBM capacity update", link="https://example.com/1", summary="GPU and HBM3E demand"),
            SimpleNamespace(title="Random lifestyle post", link="https://example.com/2", summary="travel and food"),
            SimpleNamespace(title="Another random post", link="https://example.com/3", summary="music"),
        ]
        return SimpleNamespace(
            bozo=False,
            entries=entries,
            feed={"link": "https://example.com"},
        )

    monkeypatch.setattr(feed_parser.aiohttp, "ClientSession", _FakeSession)
    monkeypatch.setattr(feed_parser.feedparser, "parse", fake_parse)

    result = await feed_parser.fetch_feed(
        "NoDateFeed",
        "https://example.com/rss",
        missing_date_fallback_ratio=0.8,
        missing_date_fallback_latest_n=2,
    )

    assert result["status"] == "success"
    assert len(result["posts"]) == 1
    assert result["posts"][0]["title"] == "HBM capacity update"
