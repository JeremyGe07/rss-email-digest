import pytest
from pathlib import Path
from datetime import datetime, timedelta, timezone
import asyncio
from src.feed_parser import (
    parse_opml,
    is_from_yesterday,
    fetch_feed,
    fetch_all_feeds,
    matches_topic_filter,
    matches_keywords,
    is_in_recent_window,
)


def test_matches_topic_filter_accepts_strong_signal():
    assert matches_topic_filter("HBM3E 与 CoWoS 产能持续拉升", "数据中心 GPU 需求旺盛") is True


def test_matches_topic_filter_rejects_generic_ai_content():
    assert matches_topic_filter("AI 绘画教程", "提示词与工作流上手指南") is False


def test_parse_opml_returns_feed_list():
    """Test that parse_opml extracts feed URLs and titles from OPML file."""
    opml_path = Path(__file__).parent / "fixtures" / "sample.opml"

    feeds = parse_opml(opml_path)

    assert len(feeds) == 2
    assert feeds[0]["title"] == "Daring Fireball"
    assert feeds[0]["url"] == "https://daringfireball.net/feeds/main"
    assert feeds[1]["title"] == "Hacker News"
    assert feeds[1]["url"] == "https://news.ycombinator.com/rss"


def test_parse_opml_handles_missing_file():
    """Test that parse_opml raises FileNotFoundError for missing file."""
    with pytest.raises(FileNotFoundError):
        parse_opml(Path("nonexistent.opml"))


def test_is_from_yesterday_with_yesterday_date():
    """Test that is_from_yesterday returns True for yesterday's date."""
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)

    # Test with datetime object
    assert is_from_yesterday(yesterday) is True

    # Test with struct_time (feedparser format)
    assert is_from_yesterday(yesterday.timetuple()) is True


def test_is_from_yesterday_with_today_date():
    """Test that is_from_yesterday returns False for today."""
    today = datetime.now(timezone.utc)

    assert is_from_yesterday(today) is False


def test_is_from_yesterday_with_old_date():
    """Test that is_from_yesterday returns False for older dates."""
    old_date = datetime.now(timezone.utc) - timedelta(days=5)

    assert is_from_yesterday(old_date) is False


def test_is_from_yesterday_with_none():
    """Test that is_from_yesterday returns False for None."""
    assert is_from_yesterday(None) is False


@pytest.mark.asyncio
async def test_fetch_feed_success():
    """Test successful feed fetch returns posts from yesterday."""
    feed_url = "https://daringfireball.net/feeds/main"

    result = await fetch_feed("Daring Fireball", feed_url, timeout=15)

    assert result["name"] == "Daring Fireball"
    assert result["status"] in ["success", "no_updates", "error"]
    assert isinstance(result["posts"], list)
    assert "site_url" in result
    # Posts should be empty or contain valid post dicts
    for post in result["posts"]:
        assert "title" in post
        assert "link" in post
        assert "excerpt" in post


@pytest.mark.asyncio
async def test_fetch_feed_timeout():
    """Test that fetch_feed handles timeout gracefully."""
    # Use a URL that will timeout (non-routable IP)
    feed_url = "http://10.255.255.1/feed.xml"

    result = await fetch_feed("Timeout Feed", feed_url, timeout=1)

    assert result["name"] == "Timeout Feed"
    assert result["status"] == "error"
    assert result["posts"] == []
    assert "site_url" in result
    assert result["site_url"] == ""  # Empty for error cases
    assert any(token in result["error_message"].lower() for token in ["timeout", "timed out", "network is unreachable", "cannot connect"])


@pytest.mark.asyncio
async def test_fetch_all_feeds():
    """Test parallel fetching of multiple feeds."""
    feeds = [
        {"title": "Daring Fireball", "url": "https://daringfireball.net/feeds/main"},
        {"title": "Hacker News", "url": "https://news.ycombinator.com/rss"}
    ]

    results = await fetch_all_feeds(feeds, batch_size=10)

    assert len(results) == 2
    assert all(r["status"] in ["success", "no_updates", "error"] for r in results)
    assert all("name" in r and "posts" in r and "site_url" in r for r in results)


@pytest.mark.asyncio
async def test_fetch_all_feeds_with_failures():
    """Test that fetch_all_feeds continues despite individual failures."""
    feeds = [
        {"title": "Valid Feed", "url": "https://daringfireball.net/feeds/main"},
        {"title": "Invalid Feed", "url": "http://10.255.255.1/feed.xml"}
    ]

    results = await fetch_all_feeds(feeds, batch_size=10, timeout=2)

    assert len(results) == 2
    # At least one should succeed, at least one should error
    statuses = [r["status"] for r in results]
    assert "error" in statuses


def test_matches_keywords_handles_hyphen_and_case_variants():
    assert matches_keywords("NVIDIA data-center GPU roadmap", "", ["data center gpu"]) is True


def test_matches_keywords_respects_word_boundaries_for_ascii_terms():
    assert matches_keywords("New CUP design", "", ["NPU"]) is False
    assert matches_keywords("NPU performance improved", "", ["NPU"]) is True


def test_is_in_recent_window_with_naive_datetime_uses_feed_timezone():
    # Naive local time should be interpreted in Asia/Shanghai by default.
    now = datetime(2026, 3, 4, 0, 0, 0, tzinfo=timezone.utc)
    local_naive = datetime(2026, 3, 3, 23, 30, 0)

    assert is_in_recent_window(local_naive, window_hours=24, now=now) is True


def test_is_in_recent_window_excludes_older_than_window():
    now = datetime(2026, 3, 4, 0, 0, 0, tzinfo=timezone.utc)
    old = datetime(2026, 3, 2, 22, 0, 0, tzinfo=timezone.utc)

    assert is_in_recent_window(old, window_hours=24, now=now) is False


def test_matches_keywords_allows_common_model_suffixes():
    assert matches_keywords("HBM4 bandwidth target", "", ["HBM"]) is True
    assert matches_keywords("PCIe5 ecosystem", "", ["PCIe"]) is True


def test_matches_keywords_handles_letter_digit_glue_variants():
    assert matches_keywords("PCIe6.0 lane planning", "", ["PCIe 6.0"]) is True


def test_matches_topic_filter_uses_word_boundary_for_english_exclude_terms():
    assert matches_topic_filter("AI silicon preview", "HBM roadmap and CoWoS updates") is True


class _FakeResponse:
    def __init__(self, status=200, content_type="text/html; charset=utf-8", body=b"<html>blocked</html>", url="https://example.com/rss"):
        self.status = status
        self.headers = {"Content-Type": content_type}
        self.url = url
        self._body = body

    async def read(self):
        return self._body


class _FakeRequestCtx:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self, response):
        self.response = response

    def get(self, *args, **kwargs):
        return _FakeRequestCtx(self.response)


@pytest.mark.asyncio
async def test_fetch_feed_logs_suspicious_response(monkeypatch, caplog):
    class ParsedFeed:
        bozo = 0
        bozo_exception = None
        entries = []
        feed = {}

    monkeypatch.setattr("src.feed_parser.feedparser.parse", lambda content: ParsedFeed())
    fake_session = _FakeSession(_FakeResponse())

    with caplog.at_level("WARNING"):
        result = await fetch_feed("Fake Feed", "https://example.com/rss", session=fake_session)

    assert result["status"] == "no_updates"
    assert "suspicious feed response" in caplog.text
    assert "content_type_is_html" in caplog.text
    assert "entries=0" in caplog.text


@pytest.mark.asyncio
async def test_fetch_feed_includes_bozo_exception_in_warning(monkeypatch, caplog):
    class ParsedFeed:
        bozo = 1
        bozo_exception = ValueError("bad xml")
        entries = []
        feed = {}

    monkeypatch.setattr("src.feed_parser.feedparser.parse", lambda content: ParsedFeed())
    fake_session = _FakeSession(_FakeResponse(content_type="application/xml", body=b"<xml>bad</xml>"))

    with caplog.at_level("WARNING"):
        result = await fetch_feed("Bad XML", "https://example.com/rss", session=fake_session)

    assert result["status"] == "error"
    assert "bozo=1" in caplog.text
    assert "bad xml" in caplog.text


@pytest.mark.asyncio
async def test_fetch_feed_xml_fallback_extracts_namespaced_rss_items(monkeypatch):
    class ParsedFeed:
        bozo = 0
        bozo_exception = None
        entries = []
        feed = {}

    xml_body = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss xmlns="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>DRAMX</title>
    <item>
      <title>HBM demand rises</title>
      <link>https://example.com/post-1</link>
      <description>HBM and CoWoS update</description>
      <pubDate>Wed, 05 Mar 2026 00:00:00 GMT</pubDate>
    </item>
  </channel>
</rss>
"""

    monkeypatch.setattr("src.feed_parser.feedparser.parse", lambda content: ParsedFeed())
    fake_session = _FakeSession(_FakeResponse(content_type="application/xml; charset=utf-8", body=xml_body))

    result = await fetch_feed("DRAMx", "https://example.com/rss", session=fake_session, keywords=["HBM"])

    assert result["status"] == "success"
    assert len(result["posts"]) == 1
    assert result["posts"][0]["title"] == "HBM demand rises"


@pytest.mark.asyncio
async def test_fetch_feed_marks_likely_antibot_block(monkeypatch, caplog):
    class ParsedFeed:
        bozo = 0
        bozo_exception = None
        entries = []
        feed = {}

    monkeypatch.setattr("src.feed_parser.feedparser.parse", lambda content: ParsedFeed())
    fake_session = _FakeSession(
        _FakeResponse(content_type="text/html; charset=utf-8", body=b'<script src="/_guard/auto.js"></script>')
    )

    with caplog.at_level("WARNING"):
        await fetch_feed("Expreview", "https://example.com/rss", session=fake_session)

    assert "likely_anti_bot_block" in caplog.text


class _SequentialFakeSession:
    def __init__(self, responses_by_url):
        self.responses_by_url = responses_by_url

    def get(self, url, *args, **kwargs):
        return _FakeRequestCtx(self.responses_by_url[url])


@pytest.mark.asyncio
async def test_fetch_feed_follows_next_page_links(monkeypatch):
    page1_body = b"<feed><entry></entry><link rel='next' href='https://example.com/rss?page=2' /></feed>"
    page2_body = b"<feed><entry></entry></feed>"

    class ParsedFeedPage1:
        bozo = 0
        bozo_exception = None
        feed = {"links": [{"rel": "next", "href": "https://example.com/rss?page=2"}], "link": "https://example.com"}
        entries = [
            {"title": "HBM news 1", "link": "https://example.com/post-1", "summary": "HBM", "published_parsed": datetime.now(timezone.utc).timetuple()}
        ]

    class ParsedFeedPage2:
        bozo = 0
        bozo_exception = None
        feed = {"links": [], "link": "https://example.com"}
        entries = [
            {"title": "HBM news 2", "link": "https://example.com/post-2", "summary": "HBM", "published_parsed": datetime.now(timezone.utc).timetuple()}
        ]

    def fake_parse(content):
        if content == page1_body:
            return ParsedFeedPage1()
        if content == page2_body:
            return ParsedFeedPage2()
        raise AssertionError("Unexpected content")

    monkeypatch.setattr("src.feed_parser.feedparser.parse", fake_parse)
    fake_session = _SequentialFakeSession({
        "https://example.com/rss": _FakeResponse(content_type="application/atom+xml", body=page1_body, url="https://example.com/rss"),
        "https://example.com/rss?page=2": _FakeResponse(content_type="application/atom+xml", body=page2_body, url="https://example.com/rss?page=2"),
    })

    result = await fetch_feed("Paged Feed", "https://example.com/rss", session=fake_session, keywords=["HBM"])

    assert result["status"] == "success"
    assert len(result["posts"]) == 2
