"""RSS feed parser module."""
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import time
import aiohttp
import feedparser
import asyncio
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_FETCH_ACCEPT = "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8"
DEFAULT_FETCH_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 RSSDigestBot/1.0"
)


DEFAULT_AI_SEMICONDUCTOR_KEYWORDS = [
    "AI芯片", "晶片", "加速卡", "AI加速卡", "训练卡", "推理卡", "算力卡", "智算卡", "计算卡", "加速器",
    "GPU", "NPU", "LPU", "data center gpu", "accelerator", "AI accelerator", "ASIC", "TPU", "DPU", "XPU",
    "H100", "H200", "B200", "GB200", "Blackwell", "Hopper", "MI300", "Gaudi", "Trainium", "Inferentia",
    "国产GPU", "摩尔线程", "壁仞", "天数智芯", "沐曦", "景嘉微", "寒武纪", "昆仑芯", "昇腾", "海光",
    "龙芯", "兆芯", "飞腾", "鲲鹏", "HBM", "HBM2e", "HBM3", "HBM3E", "CoWoS", "SoIC", "InFO",
    "Foveros", "EMIB", "Chiplet", "UCIe", "2.5D", "3D封装", "TSV", "interposer", "先进封装", "NVLink",
    "InfiniBand", "RoCE", "CXL", "PCIe 6.0", "PCIe 5.0", "800G", "硅光", "光模块", "液冷",
]

DEFAULT_TOPIC_FILTER = {
    "threshold_default": 6,
    "require_strong_hit": True,
    "title_strong_direct_accept": True,
    "weights": {"strong": 6, "medium": 2, "weak": 1},
    "strong": DEFAULT_AI_SEMICONDUCTOR_KEYWORDS,
    "medium": [
        "台积电", "TSMC", "三星代工", "Intel Foundry", "foundry", "制程", "EUV", "High-NA", "3nm", "2nm",
        "GAA", "背面供电", "良率", "tape-out", "流片", "掩膜", "光刻胶", "EDA", "Synopsys", "Cadence",
        "Siemens EDA", "DRC", "LVS", "PDK", "封装产能", "CoWoS产能", "HBM产能", "ABF", "inference",
        "training", "推理", "训练", "数据中心", "AI 服务器", "训练集群", "推理集群", "架构",
        # 存储/内存
        "存储", "内存", "闪存", "固态", "SSD", "DRAM", "NAND", "NOR", "eMMC", "UFS",
        "memory", "storage", "flash", "ssd", "dram", "nand", "nor",
        # 晶圆/制造/光刻/材料
        "晶圆", "wafer", "wafer fab", "fab",
        "光刻", "光刻机", "lithography", "scanner",
        "掩膜", "mask", "mask shop",
        "刻蚀", "etch", "etching",
        "沉积", "deposition", "CVD", "PVD",
        "CMP", "良率", "yield",
        "封测", "OSAT",
        # 供应侧更具体（替代“供应链”）
        "交付", "缺货", "供给", "扩产", "产能", "lead time", "shortage", "capacity", "ramp",
    ],
    "weak": ["CUDA", "ROCm", "oneAPI", "TensorRT", "OpenXLA", "编译器", "驱动"],
    "exclude": [
        "提示词", "prompt", "教程", "使用技巧", "上手", "AI绘画", "AIGC", "文生图", "视频生成", "聊天机器人",
        "应用", "插件", "工作流", "手机", "平板", "耳机", "相机", "手表", "家电", "评测", "开箱", "跑分",
        "游戏", "电竞", "车机", "智驾", "自动驾驶","车型","雷达",
        # 英文强排除词：泛 AI 内容
        "prompting", "prompt engineer", "prompt engineering", "how to", "tutorial", "guide", "walkthrough",
        "tips", "tricks", "ai tool", "ai tools",
        # 英文强排除词：消费硬件水文
        "review", "hands-on", "unboxing", "benchmark", "fps", "gaming", "smartphone", "phone", "tablet",
        "earbuds", "camera", "smartwatch", "laptop", "headphone",
        # 英文强排除词：泛应用/产品经理类
        "plugin", "workflow", "productivity",
        # 消费电子/移动端噪音
        "android", "ios", "airtag", "luggage", "smart home", "wearable",
        "launch event", "price", "discount",
    ],
}


def _normalize_text_for_matching(text: str) -> str:
    """Normalize text for more robust keyword matching."""
    text = (text or "").lower()
    text = re.sub(r"[\-_/]+", " ", text)
    text = re.sub(r"[^\w\s一-鿿]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _keyword_to_regex(keyword: str) -> re.Pattern:
    """Build regex pattern for a keyword with word boundaries when useful."""
    norm_keyword = _normalize_text_for_matching(keyword)
    escaped = re.escape(norm_keyword)

    # For ASCII-ish terms use letter boundaries; for CJK, plain substring is usually better.
    # This allows model suffixes like HBM4 / PCIe5 while still avoiding CUP->NPU false positives.
    if re.fullmatch(r"[a-z0-9. ]+", norm_keyword):
        pattern = rf"(?<![a-z]){escaped}(?![a-z])"
    else:
        pattern = escaped

    return re.compile(pattern)


def matches_keywords(title: str, excerpt: str, keywords: List[str]) -> bool:
    """Return True if title/excerpt matches any keyword after normalization."""
    if not keywords:
        return True

    haystack = _normalize_text_for_matching(f"{title} {excerpt}")
    return any(_keyword_to_regex(keyword).search(haystack) for keyword in keywords if keyword and keyword.strip())


def matches_topic_filter(title: str, excerpt: str, topic_filter: Dict = None) -> bool:
    """Return True when title/excerpt matches strict AI chip filtering rules."""
    config = topic_filter or DEFAULT_TOPIC_FILTER
    text = f"{title} {excerpt}".lower()
    title_lower = title.lower()

    if any(term.lower() in text for term in config.get("exclude", [])):
        return False

    strong_hits = [term for term in config.get("strong", []) if term.lower() in text]

    if config.get("title_strong_direct_accept", False) and any(
        term.lower() in title_lower for term in config.get("strong", [])
    ):
        return True

    if config.get("require_strong_hit", False) and not strong_hits:
        return False

    weights = config.get("weights", {"strong": 6, "medium": 2, "weak": 1})
    score = len(strong_hits) * weights.get("strong", 6)
    score += sum(1 for term in config.get("medium", []) if term.lower() in text) * weights.get("medium", 2)
    score += sum(1 for term in config.get("weak", []) if term.lower() in text) * weights.get("weak", 1)

    return score >= config.get("threshold_default", 6)


def parse_opml(opml_path: Path) -> List[Dict[str, str]]:
    """
    Parse OPML file and extract RSS feed URLs and titles.

    Args:
        opml_path: Path to OPML file

    Returns:
        List of dicts with 'title' and 'url' keys

    Raises:
        FileNotFoundError: If OPML file doesn't exist
    """
    if not opml_path.exists():
        raise FileNotFoundError(f"OPML file not found: {opml_path}")

    tree = ET.parse(opml_path)
    root = tree.getroot()

    feeds = []
    # Find all outline elements with xmlUrl attribute (RSS feeds)
    for outline in root.findall(".//outline[@xmlUrl]"):
        feeds.append({
            "title": outline.get("text") or outline.get("title"),
            "url": outline.get("xmlUrl"),
            "html_url": outline.get("htmlUrl", "")
        })

    return feeds


def _normalize_entry_datetime(
    date_value: Union[datetime, time.struct_time, None],
    naive_timezone: str = "Asia/Shanghai",
) -> Union[datetime, None]:
    """Normalize entry datetime to UTC; infer timezone for naive datetimes."""
    if date_value is None:
        return None

    if isinstance(date_value, time.struct_time):
        normalized = datetime(*date_value[:6], tzinfo=timezone.utc)
    elif isinstance(date_value, datetime):
        normalized = date_value
    else:
        return None

    if normalized.tzinfo is None:
        try:
            normalized = normalized.replace(tzinfo=ZoneInfo(naive_timezone))
        except Exception:
            normalized = normalized.replace(tzinfo=timezone.utc)

    return normalized.astimezone(timezone.utc)


def is_in_recent_window(
    date_value: Union[datetime, time.struct_time, None],
    window_hours: int = 24,
    now: Union[datetime, None] = None,
    naive_timezone: str = "Asia/Shanghai",
) -> bool:
    """Check if date is within the last N hours (rolling window)."""
    normalized = _normalize_entry_datetime(date_value, naive_timezone=naive_timezone)
    if normalized is None:
        return False

    current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    cutoff = current - timedelta(hours=window_hours)
    return cutoff <= normalized <= current


def is_from_yesterday(date_value: Union[datetime, time.struct_time, None]) -> bool:
    """Backward-compatible UTC yesterday check used by existing tests/helpers."""
    normalized = _normalize_entry_datetime(date_value, naive_timezone="UTC")
    if normalized is None:
        return False

    now = datetime.now(timezone.utc)
    yesterday = (now - timedelta(days=1)).date()
    return normalized.date() == yesterday


async def fetch_feed(
    feed_name: str,
    feed_url: str,
    timeout: int = 15,
    html_url: str = "",
    keywords: List[str] = None,
    window_hours: int = 24,
    naive_timezone: str = "Asia/Shanghai",
    missing_date_fallback_ratio: float = 0.8,
    missing_date_fallback_latest_n: int = 3,
) -> Dict:
    """
    Fetch RSS feed and extract yesterday's posts.

    Args:
        feed_name: Display name for the feed
        feed_url: RSS feed URL
        timeout: Request timeout in seconds
        html_url: Website URL from OPML (fallback for error cases)

    Returns:
        Dict with keys: name, status, posts, error_message (if error)
    """
    if keywords is None:
        keywords = DEFAULT_AI_SEMICONDUCTOR_KEYWORDS

    try:
        request_headers = {
            "Accept": DEFAULT_FETCH_ACCEPT,
            "User-Agent": os.getenv("RSS_FETCH_USER_AGENT", DEFAULT_FETCH_USER_AGENT),
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                feed_url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers=request_headers,
            ) as response:
                if response.status >= 400:
                    return {
                        "name": feed_name,
                        "status": "error",
                        "posts": [],
                        "error_message": f"HTTP {response.status}",
                        "site_url": html_url
                    }
                response_content_type = response.headers.get("Content-Type", "")
                response_final_url = str(response.url)
                content = await response.read()

        # Parse feed content from raw bytes for better encoding handling
        feed = feedparser.parse(content)

        # bozo often means malformed XML, but many feeds still provide usable entries.
        if feed.bozo and not feed.entries:
            return {
                "name": feed_name,
                "status": "error",
                "posts": [],
                "error_message": f"Invalid feed format: {feed.bozo_exception}",
                "site_url": html_url
            }

        if feed.bozo:
            logger.warning(f"{feed_name}: bozo feed parsed with entries, continue - {feed.bozo_exception}")

        # Extract site URL from feed metadata
        site_url = feed.feed.get("link", "") if hasattr(feed, "feed") else ""

        total_entries = len(feed.entries)
        if total_entries == 0:
            logger.info(
                "%s: feed returned 0 entries (final_url=%s, content_type=%s)",
                feed_name,
                response_final_url,
                response_content_type,
            )

        # Filter for recent-window posts
        window_posts = []
        window_candidates = 0
        keyword_hits = 0
        topic_hits = 0
        missing_date = 0
        outside_window = 0
        future_date = 0
        fallback_considered = 0
        fallback_kept = 0
        missing_date_entries = []

        now_utc = datetime.now(timezone.utc)
        cutoff_utc = now_utc - timedelta(hours=window_hours)

        for entry in feed.entries:
            title = getattr(entry, "title", "(No title)")
            link = getattr(entry, "link", "")

            excerpt = ""
            if hasattr(entry, "summary"):
                excerpt = entry.summary
            elif hasattr(entry, "content") and entry.content:
                excerpt = entry.content[0].value
            excerpt = re.sub(r'<[^>]+>', '', excerpt)
            excerpt = excerpt.strip()
            if len(excerpt) > 300:
                excerpt = excerpt[:300] + "..."

            # Try published date first, fall back to updated
            pub_date = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
            if not pub_date:
                missing_date += 1
                missing_date_entries.append({"title": title, "link": link, "excerpt": excerpt})
                continue

            normalized_date = _normalize_entry_datetime(pub_date, naive_timezone=naive_timezone)
            if not normalized_date:
                missing_date += 1
                missing_date_entries.append({"title": title, "link": link, "excerpt": excerpt})
                continue

            if normalized_date > now_utc:
                future_date += 1
                outside_window += 1
                continue
            if normalized_date < cutoff_utc:
                outside_window += 1
                continue

            window_candidates += 1

            keyword_matched = matches_keywords(title, excerpt, keywords)
            topic_matched = matches_topic_filter(title, excerpt)
            if keyword_matched:
                keyword_hits += 1
            if topic_matched:
                topic_hits += 1

            if keyword_matched and topic_matched:
                window_posts.append({
                    "title": title,
                    "link": link,
                    "excerpt": excerpt,
                    "_dedupe_scope": "window",
                })

        missing_ratio = (missing_date / total_entries) if total_entries else 0.0
        should_use_missing_fallback = (
            window_candidates == 0
            and missing_date_entries
            and missing_ratio >= missing_date_fallback_ratio
            and missing_date_fallback_latest_n > 0
        )

        if should_use_missing_fallback:
            fallback_slice = missing_date_entries[:missing_date_fallback_latest_n]
            fallback_considered = len(fallback_slice)
            for post in fallback_slice:
                keyword_matched = matches_keywords(post["title"], post["excerpt"], keywords)
                topic_matched = matches_topic_filter(post["title"], post["excerpt"])
                if keyword_matched:
                    keyword_hits += 1
                if topic_matched:
                    topic_hits += 1
                if keyword_matched and topic_matched:
                    fallback_kept += 1
                    post["_dedupe_scope"] = "fallback_missing_date"
                    window_posts.append(post)

        status = "success" if window_posts else "no_updates"
        logger.info(
            "%s: %d posts kept (entries=%d, window candidates=%d, missing_date=%d, outside_window=%d, future_date=%d, fallback_considered=%d, fallback_kept=%d, keyword_hits=%d, topic_hits=%d)",
            feed_name,
            len(window_posts),
            total_entries,
            window_candidates,
            missing_date,
            outside_window,
            future_date,
            fallback_considered,
            fallback_kept,
            keyword_hits,
            topic_hits,
        )

        return {
            "name": feed_name,
            "status": status,
            "posts": window_posts,
            "site_url": site_url
        }

    except asyncio.TimeoutError:
        logger.warning(f"{feed_name}: Timeout after {timeout}s")
        return {
            "name": feed_name,
            "status": "error",
            "posts": [],
            "error_message": f"Timeout after {timeout}s",
            "site_url": html_url
        }
    except Exception as e:
        logger.error(f"{feed_name}: Error - {str(e)}")
        return {
            "name": feed_name,
            "status": "error",
            "posts": [],
            "error_message": str(e),
            "site_url": html_url
        }


async def fetch_all_feeds(
    feeds: List[Dict[str, str]],
    batch_size: int = 10,
    timeout: int = 15,
    keywords: List[str] = None,
    window_hours: int = 24,
    naive_timezone: str = "Asia/Shanghai",
    missing_date_fallback_ratio: float = 0.8,
    missing_date_fallback_latest_n: int = 3,
) -> List[Dict]:
    """
    Fetch multiple RSS feeds in parallel batches.

    Args:
        feeds: List of feed dicts with 'title' and 'url' keys
        batch_size: Number of feeds to fetch concurrently
        timeout: Timeout per feed in seconds

    Returns:
        List of feed result dicts. Length matches input feeds list,
        with error results for feeds that fail.
    """
    results = []

    logger.info(f"Fetching {len(feeds)} feeds in batches of {batch_size}...")

    # Process feeds in batches to avoid overwhelming the system
    for i in range(0, len(feeds), batch_size):
        batch = feeds[i:i + batch_size]
        tasks = [
            fetch_feed(
                feed["title"],
                feed["url"],
                timeout,
                feed.get("html_url", ""),
                keywords,
                window_hours,
                naive_timezone,
                missing_date_fallback_ratio,
                missing_date_fallback_latest_n,
            )
            for feed in batch
        ]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and add to results
        for j, result in enumerate(batch_results):
            if isinstance(result, Exception):
                feed = batch[j]
                logger.error(f"{feed['title']}: Unexpected error - {result}")
                results.append({
                    "name": feed["title"],
                    "status": "error",
                    "posts": [],
                    "error_message": f"Unexpected error: {str(result)}",
                    "site_url": feed.get("html_url", "")
                })
            else:
                results.append(result)

    logger.info(f"Completed fetching {len(results)} feeds")
    return results
