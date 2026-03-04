"""Main entry point for RSS Daily Digest."""
import asyncio
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from email.utils import getaddresses
from src.feed_parser import parse_opml, fetch_all_feeds, DEFAULT_AI_SEMICONDUCTOR_KEYWORDS
from src.email_generator import create_email_message, send_email
from src.translator import translate_feed_results
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
def _post_fingerprint(feed_name: str, post: dict) -> str:
    link = (post.get("link") or "").strip()
    if link:
        return f"link::{link}"
    title = (post.get("title") or "").strip()
    excerpt = (post.get("excerpt") or "").strip()
    digest = hashlib.sha1(f"{feed_name}\n{title}\n{excerpt}".encode("utf-8")).hexdigest()
    return f"hash::{digest}"
def _load_seen_posts(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}
def _prune_seen_posts(seen: dict, ttl_days: int) -> dict:
    if ttl_days <= 0:
        return seen
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    pruned = {}
    for k, iso in seen.items():
        try:
            ts = datetime.fromisoformat(iso)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts >= cutoff:
                pruned[k] = ts.astimezone(timezone.utc).isoformat()
        except Exception:
            continue
    return pruned
def _filter_seen_posts(feed_results: list, seen: dict) -> tuple[list, int, int]:
    total_before = 0
    removed = 0
    for feed in feed_results:
        posts = feed.get("posts", [])
        total_before += len(posts)
        fresh_posts = []
        for post in posts:
            fp = _post_fingerprint(feed.get("name", ""), post)
            if fp in seen:
                removed += 1
                continue
            fresh_posts.append(post)
        feed["posts"] = fresh_posts
        if feed.get("status") == "success" and not fresh_posts:
            feed["status"] = "no_updates"
    return feed_results, total_before, removed
def _update_seen_posts(feed_results: list, seen: dict) -> dict:
    now_iso = datetime.now(timezone.utc).isoformat()
    for feed in feed_results:
        for post in feed.get("posts", []):
            seen[_post_fingerprint(feed.get("name", ""), post)] = now_iso
    return seen
def _save_seen_posts(path: Path, seen: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(seen, ensure_ascii=False, indent=2))
async def main():
    """Main function to run RSS digest."""
    # Validate environment variables
    required_vars = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "RECIPIENT_EMAIL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    # Get configuration from environment
    smtp_host = os.getenv("SMTP_HOST")
    try:
        smtp_port = int(os.getenv("SMTP_PORT"))
    except (ValueError, TypeError):
        logger.error(f"SMTP_PORT must be a valid integer, got: {os.getenv('SMTP_PORT')}")
        sys.exit(1)
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    recipient_email = os.getenv("RECIPIENT_EMAIL")
    smtp_security = os.getenv("SMTP_SECURITY", "auto")
    keywords_env = os.getenv("TOPIC_KEYWORDS", "")
    keywords = [k.strip() for k in keywords_env.split(",") if k.strip()] or DEFAULT_AI_SEMICONDUCTOR_KEYWORDS
    enable_translation = os.getenv("ENABLE_TRANSLATION", "true").strip().lower() in {"1", "true", "yes", "on"}
    try:
        filter_window_hours = int(os.getenv("FILTER_WINDOW_HOURS", "24"))
    except (ValueError, TypeError):
        logger.error(f"FILTER_WINDOW_HOURS must be a valid integer, got: {os.getenv('FILTER_WINDOW_HOURS')}")
        sys.exit(1)
    feed_date_timezone = os.getenv("FEED_DATE_TIMEZONE", "Asia/Shanghai")
    try:
        missing_date_fallback_ratio = float(os.getenv("MISSING_DATE_FALLBACK_RATIO", "0.8"))
    except (ValueError, TypeError):
        logger.error(f"MISSING_DATE_FALLBACK_RATIO must be a valid float, got: {os.getenv('MISSING_DATE_FALLBACK_RATIO')}")
        sys.exit(1)
    try:
        missing_date_fallback_latest_n = int(os.getenv("MISSING_DATE_FALLBACK_LATEST_N", "3"))
    except (ValueError, TypeError):
        logger.error(f"MISSING_DATE_FALLBACK_LATEST_N must be a valid integer, got: {os.getenv('MISSING_DATE_FALLBACK_LATEST_N')}")
        sys.exit(1)
    seen_posts_file = Path(os.getenv("SEEN_POSTS_FILE", ".cache/rss-seen-posts.json"))
    try:
        seen_posts_ttl_days = int(os.getenv("SEEN_POSTS_TTL_DAYS", "30"))
    except (ValueError, TypeError):
        logger.error(f"SEEN_POSTS_TTL_DAYS must be a valid integer, got: {os.getenv('SEEN_POSTS_TTL_DAYS')}")
        sys.exit(1)
    recipient_list = [email for _, email in getaddresses([recipient_email]) if email]
    if not recipient_list:
        logger.error("RECIPIENT_EMAIL must contain at least one valid recipient")
        sys.exit(1)
    recipient_header = ", ".join(recipient_list)
    try:
        # Parse OPML file
        opml_path = Path(__file__).parent.parent / "feeds.opml"
        if not opml_path.exists():
            logger.error(f"OPML file not found: {opml_path}")
            logger.info("Create a feeds.opml file in the repository root with your RSS feeds")
            sys.exit(1)
        logger.info(f"Parsing OPML file: {opml_path}")
        feeds = parse_opml(opml_path)
        logger.info(f"Found {len(feeds)} feeds")
        # Fetch all feeds
        feed_results = await fetch_all_feeds(
            feeds,
            batch_size=10,
            timeout=15,
            keywords=keywords,
            window_hours=filter_window_hours,
            naive_timezone=feed_date_timezone,
            missing_date_fallback_ratio=missing_date_fallback_ratio,
            missing_date_fallback_latest_n=missing_date_fallback_latest_n,
        )
        seen_posts = _prune_seen_posts(_load_seen_posts(seen_posts_file), seen_posts_ttl_days)
        feed_results, total_before_dedupe, removed_as_seen = _filter_seen_posts(feed_results, seen_posts)
        if removed_as_seen:
            logger.info(
                "Deduplicated already-sent posts: removed=%d, remaining=%d",
                removed_as_seen,
                total_before_dedupe - removed_as_seen,
            )
        if enable_translation:
            logger.info("Translating non-Chinese titles/excerpts to Chinese (best-effort)...")
            feed_results = translate_feed_results(feed_results)
        # Create and send email
        logger.info("Generating email...")
        msg = create_email_message(
            feed_results=feed_results,
            from_email=smtp_user,
            to_email=recipient_header
        )
        logger.info("Sending email...")
        send_email(
            msg=msg,
            smtp_host=smtp_host,
            smtp_port=smtp_port,
            smtp_user=smtp_user,
            smtp_password=smtp_password,
            smtp_security=smtp_security
        )
        seen_posts = _update_seen_posts(feed_results, seen_posts)
        _save_seen_posts(seen_posts_file, seen_posts)
        logger.info("RSS digest sent successfully!")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        sys.exit(1)
if __name__ == "__main__":
    asyncio.run(main())
