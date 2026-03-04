from src import translator


class FakeTranslator:
    def __init__(self):
        self.calls = []

    def translate(self, text: str) -> str:
        self.calls.append(text)
        mapping = {
            "New Apple Chip": "苹果新芯片",
            "Big performance jump": "性能大幅提升",
        }
        return mapping.get(text, f"翻译:{text}")


class FakeBatchTranslator(FakeTranslator):
    def __init__(self):
        super().__init__()
        self.batch_calls = []

    def translate_batch(self, texts):
        self.batch_calls.append(list(texts))
        return [self.translate(t) for t in texts]


class FakeFailingBatchTranslator(FakeTranslator):
    def __init__(self):
        super().__init__()
        self.batch_calls = 0

    def translate_batch(self, texts):
        self.batch_calls += 1
        raise RuntimeError("simulated batch failure")


def test_translate_feed_results_replaces_with_chinese(monkeypatch):
    fake = FakeTranslator()
    monkeypatch.setattr(translator, "build_translator", lambda target_lang="zh-CN": fake)

    feed_results = [
        {
            "name": "Ars Technica",
            "posts": [
                {
                    "title": "New Apple Chip",
                    "excerpt": "Big performance jump",
                }
            ],
        }
    ]

    translated = translator.translate_feed_results(feed_results)

    assert translated[0]["name"] == "Ars Technica"
    assert translated[0]["posts"][0]["title"] == "苹果新芯片"
    assert translated[0]["posts"][0]["excerpt"] == "性能大幅提升"


def test_maybe_translate_text_uses_cache():
    fake = FakeTranslator()
    cache = {}

    first = translator.maybe_translate_text("Ars Technica", fake, cache)
    second = translator.maybe_translate_text("Ars Technica", fake, cache)

    assert first == "翻译:Ars Technica"
    assert second == "翻译:Ars Technica"
    assert fake.calls == ["Ars Technica"]


def test_maybe_translate_text_skips_cjk_text():
    fake = FakeTranslator()
    cache = {}

    text = "这是中文标题"
    result = translator.maybe_translate_text(text, fake, cache)

    assert result == text
    assert fake.calls == []


def test_translate_feed_results_skips_chinese_feed(monkeypatch):
    fake = FakeTranslator()
    monkeypatch.setattr(translator, "build_translator", lambda target_lang="zh-CN": fake)

    feed_results = [
        {
            "name": "量子位 QbitAI",
            "posts": [
                {
                    "title": "New Apple Chip",
                    "excerpt": "Big performance jump",
                }
            ],
        }
    ]

    translated = translator.translate_feed_results(feed_results)

    assert translated[0]["posts"][0]["title"] == "New Apple Chip"
    assert translated[0]["posts"][0]["excerpt"] == "Big performance jump"
    assert fake.calls == []


def test_translate_feed_results_uses_batch_translation_when_available(monkeypatch):
    fake = FakeBatchTranslator()
    monkeypatch.setattr(translator, "build_translator", lambda target_lang="zh-CN": fake)
    monkeypatch.setenv("TRANSLATION_BATCH_SIZE", "3")

    feed_results = [
        {
            "name": "Ars Technica",
            "posts": [
                {"title": "New Apple Chip", "excerpt": "Big performance jump"},
                {"title": "New Apple Chip", "excerpt": "Big performance jump"},
            ],
        }
    ]

    translated = translator.translate_feed_results(feed_results)

    assert translated[0]["posts"][0]["title"] == "苹果新芯片"
    assert translated[0]["posts"][0]["excerpt"] == "性能大幅提升"
    assert len(fake.batch_calls) == 1
    assert fake.batch_calls[0] == ["New Apple Chip", "Big performance jump"]


def test_translate_texts_best_effort_falls_back_to_single_on_batch_error():
    fake = FakeFailingBatchTranslator()
    cache = {}

    translator.translate_texts_best_effort(
        texts=["New Apple Chip", "Big performance jump"],
        translator=fake,
        cache=cache,
        batch_size=8,
    )

    assert fake.batch_calls == 1
    assert cache["New Apple Chip"] == "苹果新芯片"
    assert cache["Big performance jump"] == "性能大幅提升"
    assert fake.calls == ["New Apple Chip", "Big performance jump"]
