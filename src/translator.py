"""Best-effort translation helpers for digest content."""
from __future__ import annotations

from typing import Dict, List, Protocol
import json
import logging
import os
import re
import time

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
        self.max_retries = int(os.getenv("GEMINI_TRANSLATION_MAX_RETRIES", "3"))

    def _parse_retry_delay_seconds(self, error: Exception) -> float | None:
        text = str(error)
        # SDK error strings often include "Please retry in 56.2s".
        retry_in_match = re.search(r"retry in\s*([0-9]+(?:\.[0-9]+)?)s", text, flags=re.IGNORECASE)
        if retry_in_match:
            return float(retry_in_match.group(1))

        retry_after_match = re.search(r"retry-after[:=]\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
        if retry_after_match:
            return float(retry_after_match.group(1))

        return None

    def _generate_with_retry(self, prompt: str):
        for attempt in range(self.max_retries + 1):
            try:
                return self.client.models.generate_content(model=self.model, contents=prompt)
            except Exception as e:
                is_last_attempt = attempt >= self.max_retries
                is_quota = "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e)
                if not is_quota or is_last_attempt:
                    raise

                wait_seconds = self._parse_retry_delay_seconds(e)
                if wait_seconds is None:
                    wait_seconds = min(60, 10 * (attempt + 1))
                wait_seconds = max(1.0, wait_seconds)
                logger.warning(
                    "Gemini quota/rate limited; sleeping %.1fs before retry (%d/%d)",
                    wait_seconds,
                    attempt + 1,
                    self.max_retries,
                )
                time.sleep(wait_seconds)

        raise RuntimeError("Unexpected retry flow termination")

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
        response = self._generate_with_retry(prompt)
        translated = (response.text or "").strip()
        if not translated:
            raise ValueError("Gemini returned empty translation")
        return translated

    def translate_batch(self, texts: List[str]) -> List[str]:
        """Translate multiple strings in one request, preserving input order."""
        if not texts:
            return []

        payload = [{"id": i, "text": t} for i, t in enumerate(texts)]
        prompt = (
            "你是一名专业科技编辑，请将输入数组中的每条 text 翻译成简体中文。\n"
            "必须严格返回 JSON 数组，且每个元素包含 id 和 translation 字段。\n"
            "要求：\n"
            "1) 不要输出除 JSON 之外的任何文字；\n"
            "2) 保留产品名、公司名、芯片型号等专有名词；\n"
            "3) 忠实原文，不补充新信息。\n\n"
            "输入：\n"
            f"{json.dumps(payload, ensure_ascii=False)}"
        )

        response = self._generate_with_retry(prompt)
        raw_text = (response.text or "").strip()
        if not raw_text:
            raise ValueError("Gemini returned empty batch translation")

        # Handle optional markdown code fence from model.
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)

        data = json.loads(raw_text)
        if not isinstance(data, list):
            raise ValueError("Gemini batch translation response is not a list")

        mapped: Dict[int, str] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            idx = item.get("id")
            translated = (item.get("translation") or "").strip()
            if isinstance(idx, int) and translated:
                mapped[idx] = translated

        if len(mapped) != len(texts):
            raise ValueError("Gemini batch translation response size mismatch")

        return [mapped[i] for i in range(len(texts))]


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


def translate_texts_best_effort(
    texts: List[str],
    translator: TranslatorClient,
    cache: Dict[str, str],
    max_chars: int = 1200,
    batch_size: int = 8,
) -> None:
    """Fill cache with translations using batch API when available."""
    pending: List[str] = []
    seen = set()
    for text in texts:
        if not text or contains_cjk(text):
            continue
        snippet = text[:max_chars]
        if snippet in cache or snippet in seen:
            continue
        seen.add(snippet)
        pending.append(snippet)

    if not pending:
        return

    batch_translate = getattr(translator, "translate_batch", None)
    if callable(batch_translate):
        for i in range(0, len(pending), batch_size):
            chunk = pending[i : i + batch_size]
            try:
                translated_list = batch_translate(chunk)
                if len(translated_list) != len(chunk):
                    raise ValueError("batch translation result length mismatch")
                for src, translated in zip(chunk, translated_list):
                    cache[src] = translated
            except Exception as e:
                logger.warning("Batch translation failed, fallback to single requests: %s", e)
                for src in chunk:
                    try:
                        cache[src] = translator.translate(src)
                    except Exception as inner_e:
                        logger.warning("Translation skipped for a text due to error: %s", inner_e)
    else:
        for src in pending:
            try:
                cache[src] = translator.translate(src)
            except Exception as e:
                logger.warning("Translation skipped for a text due to error: %s", e)


def translate_feed_results(feed_results: List[Dict], target_lang: str = "zh-CN") -> List[Dict]:
    """Translate post title/excerpt fields in feed results."""
    translator = build_translator(target_lang=target_lang)
    cache: Dict[str, str] = {}
    batch_size = int(os.getenv("TRANSLATION_BATCH_SIZE", "8"))

    texts_to_translate: List[str] = []
    for feed in feed_results:
        if should_skip_feed_translation(feed):
            continue
        for post in feed.get("posts", []):
            texts_to_translate.append(post.get("title", ""))
            texts_to_translate.append(post.get("excerpt", ""))

    translate_texts_best_effort(
        texts=texts_to_translate,
        translator=translator,
        cache=cache,
        batch_size=batch_size,
    )

    for feed in feed_results:
        if should_skip_feed_translation(feed):
            continue

        for post in feed.get("posts", []):
            original_title = post.get("title", "")
            title_key = original_title[:1200]
            translated_title = cache.get(title_key, original_title)
            if translated_title and translated_title != original_title:
                post["title"] = translated_title

            original_excerpt = post.get("excerpt", "")
            excerpt_key = original_excerpt[:1200]
            translated_excerpt = cache.get(excerpt_key, original_excerpt)
            if translated_excerpt and translated_excerpt != original_excerpt:
                post["excerpt"] = translated_excerpt

    return feed_results
