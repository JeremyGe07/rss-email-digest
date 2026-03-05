"""RSS feed parser module."""
import os
import re
import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path
from typing import List, Dict, Union
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import time
from urllib.parse import urljoin
import aiohttp
import feedparser
import asyncio
import logging
from email.utils import parsedate_to_datetime


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DEFAULT_FETCH_ACCEPT = "application/rss+xml, application/atom+xml, application/xml, text/xml;q=0.9, */*;q=0.8"
DEFAULT_FETCH_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 RSSDigestBot/1.0"
)
DEFAULT_DEBUG_SNIPPET_LENGTH = 200
DEFAULT_MAX_PAGES_PER_FEED = 3


DEFAULT_AI_SEMICONDUCTOR_KEYWORDS = [
    "芯片", "晶片", "加速卡", "AI加速卡", "训练卡", "推理卡", "算力卡", "智算卡", "计算卡", "加速器",
    "GPU", "NPU", "LPU", "data center gpu", "accelerator", "AI accelerator", "ASIC", "TPU", "DPU", "XPU",
    "H100", "GB200", "Blackwell", "Hopper", "Inferentia", "910", "片上",
    "国产GPU", "摩尔线程", "壁仞", "天数智芯", "沐曦", "景嘉微", "寒武纪", "昆仑芯", "昇腾", "海光",
    "龙芯", "兆芯", "飞腾", "鲲鹏", "HBM", "HBM2e", "HBM3", "HBM3E", "HBM4", "CoWoS", "SoIC", "InFO",
    "Foveros", "EMIB", "Chiplet", "UCIe", "2.5D", "3D封装", "TSV", "interposer", "先进封装", "NVLink",
    "InfiniBand", "RoCE", "CXL", "PCIe 6.0", "PCIe 5.0", "800G", "硅光", "光模块", 
]

DEFAULT_TOPIC_FILTER = {
    "threshold_default": 5,
    "require_strong_hit": True,
    "title_strong_direct_accept": True,
    "weights": {"strong": 6, "medium": 2, "weak": 1},
    "strong": DEFAULT_AI_SEMICONDUCTOR_KEYWORDS,
    "medium": [
        "台积电", "TSMC", "三星代工", "Intel Foundry", "foundry", "制程", "EUV", "High-NA", "3nm", "2nm",
        "GAA", "背面供电", "良率", "tape-out", "流片", "掩膜", "光刻胶", "EDA", "Synopsys", "Cadence",
        "Siemens EDA", "DRC", "LVS", "PDK", "封装产能", "CoWoS产能", "HBM产能", "ABF", "inference",
        "training", "推理", "训练", "数据中心", "服务器", "训练集群", "推理集群", "架构",
        # 存储/内存
        "存储", "内存", "闪存", "固态", "SSD", "DRAM", "NAND", "NOR", "eMMC", "UFS",
        "memory", "storage", "flash", "ssd", "dram", "nand", "nor", "存算",
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
    "weak": ["CUDA", "ROCm", "oneAPI", "TensorRT", "OpenXLA", "编译器", "驱动", "AI"],
    "exclude": [
        "提示词", "prompt", "教程", "使用技巧", "上手", "AI绘画", "AIGC", "文生图", "视频生成", "聊天机器人",
        "应用", "插件", "工作流", "手机", "平板", "耳机", "相机", "手表", "家电", "评测", "开箱", "跑分",
        "游戏", "电竞", "车机", "智驾", "自动驾驶", "车型", "雷达", "电视", "数据中心",
        # 英文强排除词：泛 AI 内容
        "prompting", "prompt engineer", "prompt engineering", "how to", "tutorial", "guide", "walkthrough",
        "tips", "tricks", "ai tool", "ai tools",
        # 英文强排除词：消费硬件水文
        "review", "hands-on", "unboxing", "benchmark", "fps", "gaming", "smartphone", "phone", "tablet",
        "earbuds", "camera", "smartwatch", "laptop", "headphone",
        # 英文强排除词：泛应用/产品经理类
        "plugin", "workflow", "productivity",
        # 消费电子/移动端噪音
        "android", "ios", "airtag", "luggage", "smart home", "wearable", "discount",
    ],
}


def _normalize_text_for_matching(text: str) -> str:
    """Normalize text for more robust keyword matching."""
    text = (text or "").lower()
    text = re.sub(r"[\-_/]+", " ", text)
    text = re.sub(r"([a-z])([0-9])", r"\1 \2", text)
    text = re.sub(r"([0-9])([a-z])", r"\1 \2", text)
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


def _dedupe_terms(terms: List[str]) -> List[str]:
    """Deduplicate terms while keeping their original order."""
    seen = set()
    deduped = []
    for term in terms or []:
        if not term or not term.strip():
            continue
        normalized = _normalize_text_for_matching(term)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(term)
    return deduped


@lru_cache(maxsize=128)
def _compile_patterns(terms: tuple[str, ...]) -> tuple[re.Pattern, ...]:
    """Compile matching patterns once for a term set."""
    return tuple(_keyword_to_regex(term) for term in terms)


def _to_pattern_tuple(terms: List[str]) -> tuple[re.Pattern, ...]:
    deduped_terms = tuple(_dedupe_terms(terms))
    if not deduped_terms:
        return tuple()
    return _compile_patterns(deduped_terms)


def matches_keywords(title: str, excerpt: str, keywords: List[str]) -> bool:
    """Return True if title/excerpt matches any keyword after normalization."""
    if not keywords:
        return True

    haystack = _normalize_text_for_matching(f"{title} {excerpt}")
    return any(pattern.search(haystack) for pattern in _to_pattern_tuple(keywords))


def matches_topic_filter(title: str, excerpt: str, topic_filter: Dict = None) -> bool:
    """Return True when title/excerpt matches strict AI chip filtering rules."""
    config = topic_filter or DEFAULT_TOPIC_FILTER
    text = _normalize_text_for_matching(f"{title} {excerpt}")
    title_norm = _normalize_text_for_matching(title)

    exclude_patterns = _to_pattern_tuple(config.get("exclude", []))
    strong_patterns = _to_pattern_tuple(config.get("strong", []))
    medium_patterns = _to_pattern_tuple(config.get("medium", []))
    weak_patterns = _to_pattern_tuple(config.get("weak", []))

    if any(pattern.search(text) for pattern in exclude_patterns):
        return False

    strong_hits = [pattern for pattern in strong_patterns if pattern.search(text)]

    if config.get("title_strong_direct_accept", False) and any(
        pattern.search(title_norm) for pattern in strong_patterns
    ):
        return True

    if config.get("require_strong_hit", False) and not strong_hits:
        return False

    weights = config.get("weights", {"strong": 6, "medium": 2, "weak": 1})
    score = len(strong_hits) * weights.get("strong", 6)
    score += sum(1 for pattern in medium_patterns if pattern.search(text)) * weights.get("medium", 2)
    score += sum(1 for pattern in weak_patterns if pattern.search(text)) * weights.get("weak", 1)

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

def _entry_get(entry: Union[dict, object], key: str, default=None):
    """Read entry field from feedparser objects or dict fallback entries."""
    if isinstance(entry, dict):
        return entry.get(key, default)
    return getattr(entry, key, default)


def _localname(tag: str) -> str:
    """Get local xml name from namespaced tag."""
    if not tag:
        return ""
    if "}" in tag:
        return tag.split("}", 1)[1].lower()
    return tag.lower()


def _parse_date_string(date_text: str) -> Union[time.struct_time, None]:
    """Parse common RSS/Atom date strings into struct_time."""
    if not date_text:
        return None

    try:
        parsed_dt = parsedate_to_datetime(date_text.strip())
        if parsed_dt is None:
            return None
        if parsed_dt.tzinfo is None:
            parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
        return parsed_dt.astimezone(timezone.utc).timetuple()
    except Exception:
        return None


def _extract_entries_from_xml_fallback(content: bytes) -> List[Dict]:
    """Fallback XML parsing for feeds that feedparser fails to tokenize."""
    try:
        root = ET.fromstring(content)
    except Exception:
        return []

    extracted_entries = []
    for node in root.iter():
        node_name = _localname(node.tag)
        if node_name not in ("item", "entry"):
            continue

        item = {"title": "(No title)", "link": "", "summary": ""}
        pub_date_text = ""

        for child in list(node):
            child_name = _localname(child.tag)
            child_text = (child.text or "").strip()
            if child_name == "title" and child_text:
                item["title"] = child_text
            elif child_name == "link":
                href = child.attrib.get("href", "").strip()
                item["link"] = href or child_text
            elif child_name in ("description", "summary", "content") and child_text:
                item["summary"] = child_text
            elif child_name in ("pubdate", "updated", "date") and child_text and not pub_date_text:
                pub_date_text = child_text

        parsed_date = _parse_date_string(pub_date_text)
        if parsed_date:
            item["published_parsed"] = parsed_date

        extracted_entries.append(item)

    return extracted_entries


def _extract_next_link_from_xml(content: bytes) -> Union[str, None]:
    """Try to read atom-style rel=next link directly from XML payload."""
    try:
        root = ET.fromstring(content)
    except Exception:
        return None

    for node in root.iter():
        if _localname(node.tag) != "link":
            continue
        rel = (node.attrib.get("rel") or "").strip().lower()
        href = (node.attrib.get("href") or "").strip()
        if rel == "next" and href:
            return href

    return None


def _extract_next_page_url(feed_obj, content: bytes, current_url: str) -> Union[str, None]:
    """Extract next-page URL from parsed feed metadata or raw XML."""
    link_candidates = []

    feed_meta = getattr(feed_obj, "feed", {}) if feed_obj else {}
    if isinstance(feed_meta, dict):
        link_candidates.extend(feed_meta.get("links", []) or [])

    top_links = getattr(feed_obj, "links", []) if feed_obj else []
    if top_links:
        link_candidates.extend(top_links)

    for link_obj in link_candidates:
        rel = str(_entry_get(link_obj, "rel", "")).strip().lower()
        href = str(_entry_get(link_obj, "href", "")).strip()
        if rel == "next" and href:
            return urljoin(current_url, href)

    xml_next = _extract_next_link_from_xml(content)
    if xml_next:
        return urljoin(current_url, xml_next)

    return None





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
    session: Union[aiohttp.ClientSession, None] = None,
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
        max_pages = max(1, int(os.getenv("RSS_MAX_PAGES_PER_FEED", str(DEFAULT_MAX_PAGES_PER_FEED))))
        request_headers = {
            "Accept": DEFAULT_FETCH_ACCEPT,
            "User-Agent": os.getenv("RSS_FETCH_USER_AGENT", DEFAULT_FETCH_USER_AGENT),
        }

        request_session = session
        session_owner = request_session is None
        if session_owner:
            request_session = aiohttp.ClientSession()

        snippet_length = int(os.getenv("RSS_DEBUG_SNIPPET_LENGTH", str(DEFAULT_DEBUG_SNIPPET_LENGTH)))

        try:
            entries = []
            visited_urls = set()
            paged_url = feed_url
            feed = None
            used_xml_fallback = False
            response_content_type = ""
            response_final_url = feed_url
            response_status = 0
            content = b""
            page_count = 0

            while paged_url and page_count < max_pages and paged_url not in visited_urls:
                visited_urls.add(paged_url)
                async with request_session.get(
                    paged_url,
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
                    response_status = response.status
                    content = await response.read()

                page_count += 1

                # Parse feed content from raw bytes for better encoding handling
                parsed_feed = feedparser.parse(content)
                if feed is None:
                    feed = parsed_feed

                has_rss_markers = any(marker in content[:snippet_length].decode("utf-8", errors="replace").lower() for marker in ("<rss", "<feed", "<rdf", "<channel"))
                parsed_entries = list(getattr(parsed_feed, "entries", []))
                page_used_xml_fallback = False
                if not parsed_entries and has_rss_markers and "xml" in response_content_type.lower():
                    fallback_entries = _extract_entries_from_xml_fallback(content)
                    if fallback_entries:
                        parsed_entries = fallback_entries
                        page_used_xml_fallback = True
                        logger.info("%s: xml fallback extracted %d entries from page %d", feed_name, len(parsed_entries), page_count)

                used_xml_fallback = used_xml_fallback or page_used_xml_fallback
                entries.extend(parsed_entries)

                next_page_url = _extract_next_page_url(parsed_feed, content, response_final_url)
                if not next_page_url or next_page_url in visited_urls:
                    break
                paged_url = next_page_url

            if page_count > 1:
                logger.info("%s: paginated fetch loaded %d pages (entries=%d)", feed_name, page_count, len(entries))
        finally:
            if session_owner and request_session:
                await request_session.close()

        content_head = content[:snippet_length].decode("utf-8", errors="replace").replace("\n", "\\n").replace("\r", "\\r")

        # Keep first-seen order while deduping across paginated pages.
        deduped_entries = []
        seen_entry_keys = set()
        for entry in entries:
            key = (
                str(_entry_get(entry, "id", "") or "").strip(),
                str(_entry_get(entry, "link", "") or "").strip(),
                str(_entry_get(entry, "title", "") or "").strip(),
            )
            if key in seen_entry_keys:
                continue
            seen_entry_keys.add(key)
            deduped_entries.append(entry)
        entries = deduped_entries

        suspicious_reasons = []
        lowered_head = content_head.lower()
        if "html" in response_content_type.lower() and not has_rss_markers:
            suspicious_reasons.append("content_type_is_html")
        if not entries:
            suspicious_reasons.append("entries=0")
        if getattr(feed, "bozo", 0):
            suspicious_reasons.append("bozo=1")
        if any(token in lowered_head for token in ("_guard/auto.js", "captcha", "cloudflare", "enable javascript")):
            suspicious_reasons.append("likely_anti_bot_block")

        if suspicious_reasons:
            logger.warning(
                "%s: suspicious feed response (%s, status=%s, final_url=%s, content_type=%s, bytes=%d, xml_fallback_used=%s, bozo_exception=%r, head=%s)",
                feed_name,
                ", ".join(suspicious_reasons),
                response_status,
                response_final_url,
                response_content_type,
                len(content),
                used_xml_fallback,
                getattr(feed, "bozo_exception", None),
                content_head,
            )

        # bozo often means malformed XML, but many feeds still provide usable entries.
        if feed.bozo and not entries:
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

        total_entries = len(entries)
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

        for entry in entries:
            title = _entry_get(entry, "title", "(No title)")
            link = _entry_get(entry, "link", "")

            excerpt = _entry_get(entry, "summary", "") or ""
            entry_content = _entry_get(entry, "content", None)
            if not excerpt and entry_content:
                first_content = entry_content[0] if isinstance(entry_content, (list, tuple)) else entry_content
                if isinstance(first_content, dict):
                    excerpt = first_content.get("value", "")
                else:
                    excerpt = getattr(first_content, "value", "")
            excerpt = re.sub(r'<[^>]+>', '', excerpt)
            excerpt = excerpt.strip()
            if len(excerpt) > 300:
                excerpt = excerpt[:300] + "..."

            # Try published date first, fall back to updated
            pub_date = _entry_get(entry, "published_parsed", None) or _entry_get(entry, "updated_parsed", None)
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

    connector_limit = max(batch_size * 2, 20)
    connector = aiohttp.TCPConnector(limit=connector_limit)
    async with aiohttp.ClientSession(connector=connector) as shared_session:
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
                    session=shared_session,
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
