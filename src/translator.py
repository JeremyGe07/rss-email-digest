"""Best-effort translation helpers for digest content."""
from typing import Dict, List
import logging
import re

from deep_translator import GoogleTranslator


logger = logging.getLogger(__name__)


def contains_cjk(text: str) -> bool:
    """Return True if text contains Chinese/Japanese/Korean characters."""
    return bool(re.search(r"[\u4e00-\u9fff\u3400-\u4dbf]", text or ""))


def maybe_translate_text(
    text: str,
    translator: GoogleTranslator,
    cache: Dict[str, str],
    max_chars: int = 1200,
) -> str:
    """Translate non-CJK text to Simplified Chinese with caching."""
    if not text or contains_cjk(text):
        return text

    snippet = text[:max_chars]
    if snippet in cache:
        return cache[snippet]

    translated = translator.translate(snippet)
    cache[snippet] = translated
    return translated


def translate_feed_results(feed_results: List[Dict], target_lang: str = "zh-CN") -> List[Dict]:
    """Translate post title/excerpt fields in feed results (best-effort)."""
    translator = GoogleTranslator(source="auto", target=target_lang)
    cache: Dict[str, str] = {}

    for feed in feed_results:
        for post in feed.get("posts", []):
            try:
                original_title = post.get("title", "")
                translated_title = maybe_translate_text(original_title, translator, cache)
                if translated_title and translated_title != original_title:
                    post["title"] = f"{translated_title}（原文: {original_title}）"

                original_excerpt = post.get("excerpt", "")
                translated_excerpt = maybe_translate_text(original_excerpt, translator, cache)
                if translated_excerpt and translated_excerpt != original_excerpt:
                    post["excerpt"] = f"{translated_excerpt}\n原文: {original_excerpt}"
            except Exception as e:
                logger.warning(f"Translation skipped for a post due to error: {e}")

    return feed_results
