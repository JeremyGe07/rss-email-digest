"""Best-effort translation helpers for digest content."""
from __future__ import annotations

from typing import Dict, List, Protocol
import logging
import os
import re

from deep_translator import GoogleTranslator


logger = logging.getLogger(__name__)


class TranslatorClient(Protocol):
    """Minimal protocol shared by translation providers."""

    def translate(self, text: str) -> str:
        ...


class GeminiTranslator:
    """Gemini-backed translator using google-genai."""

    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY is required for Gemini translation")

        from google import genai

        self.client = genai.Client(api_key=api_key)
        self.model = model

    def translate(self, text: str) -> str:
        prompt = (
            "你是一名专业科技编辑，请将下面内容翻译成简体中文。\n"
            "要求：\n"
            "1) 仅输出译文，不要输出解释、注释、原文或引号；\n"
            "2) 保留产品名、公司名、芯片型号（如 M5 Pro、Xe3、OpenVINO）等专有名词；\n"
            "3) 保留原有事实，不补充未出现的信息；\n"
            "4) 语气自然、简洁，适合新闻邮件阅读。\n\n"
            f"原文：\n{text}"
        )
        response = self.client.models.generate_content(model=self.model, contents=prompt)
        translated = (response.text or "").strip()
        if not translated:
            raise ValueError("Gemini returned empty translation")
        return translated


class DeepTranslatorAdapter:
    """Adapter around deep-translator to unify translator interface."""

    def __init__(self, target_lang: str = "zh-CN") -> None:
        self.translator = GoogleTranslator(source="auto", target=target_lang)

    def translate(self, text: str) -> str:
        return self.translator.translate(text)


def build_translator(target_lang: str = "zh-CN") -> TranslatorClient:
    """Build translator client, preferring Gemini when available."""
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        try:
            model = os.getenv("GEMINI_TRANSLATION_MODEL", "gemini-3-flash-preview")
            logger.info("Using Gemini translator model: %s", model)
            return GeminiTranslator(model=model)
        except Exception as e:
            logger.warning(
                "Gemini translator unavailable (%s: %s), falling back to deep-translator",
                type(e).__name__,
                e,
            )
    else:
        logger.info("GEMINI_API_KEY not set, using deep-translator fallback")

    logger.info("Using deep-translator fallback")
    return DeepTranslatorAdapter(target_lang=target_lang)


def contains_cjk(text: str) -> bool:
    """Return True if text contains Chinese/Japanese/Korean characters."""
    return bool(re.search(r"[\u4e00-\u9fff\u3400-\u4dbf]", text or ""))


def should_skip_feed_translation(feed: Dict) -> bool:
    """Return True when the feed is likely Chinese and can skip translation entirely."""
    feed_name = feed.get("name", "")
    if contains_cjk(feed_name):
        return True

    language = (feed.get("language") or "").lower()
    return language.startswith("zh")


def maybe_translate_text(
    text: str,
    translator: TranslatorClient,
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
    """Translate post title/excerpt fields in feed results."""
    translator = build_translator(target_lang=target_lang)
    cache: Dict[str, str] = {}

    for feed in feed_results:
        if should_skip_feed_translation(feed):
            continue

        for post in feed.get("posts", []):
            try:
                original_title = post.get("title", "")
                translated_title = maybe_translate_text(original_title, translator, cache)
                if translated_title and translated_title != original_title:
                    post["title"] = translated_title

                original_excerpt = post.get("excerpt", "")
                translated_excerpt = maybe_translate_text(original_excerpt, translator, cache)
                if translated_excerpt and translated_excerpt != original_excerpt:
                    post["excerpt"] = translated_excerpt
            except Exception as e:
                logger.warning("Translation skipped for a post due to error: %s", e)

    return feed_results
