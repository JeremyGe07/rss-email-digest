from src import translator


class FakeTranslator:
    def __init__(self):
        self.calls = []

    def translate(self, text: str) -> str:
        self.calls.append(text)
        mapping = {
            "Ars Technica": "Ars 科技",
            "New Apple Chip": "苹果新芯片",
            "Big performance jump": "性能大幅提升",
        }
        return mapping.get(text, f"翻译:{text}")


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

    assert translated[0]["name"] == "Ars 科技"
    assert translated[0]["posts"][0]["title"] == "苹果新芯片"
    assert translated[0]["posts"][0]["excerpt"] == "性能大幅提升"


def test_maybe_translate_text_uses_cache():
    fake = FakeTranslator()
    cache = {}

    first = translator.maybe_translate_text("Ars Technica", fake, cache)
    second = translator.maybe_translate_text("Ars Technica", fake, cache)

    assert first == "Ars 科技"
    assert second == "Ars 科技"
    assert fake.calls == ["Ars Technica"]


def test_maybe_translate_text_skips_cjk_text():
    fake = FakeTranslator()
    cache = {}

    text = "这是中文标题"
    result = translator.maybe_translate_text(text, fake, cache)

    assert result == text
    assert fake.calls == []
