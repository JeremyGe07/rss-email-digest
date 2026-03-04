from src.main import _build_keywords, DEFAULT_AI_SEMICONDUCTOR_KEYWORDS


def test_build_keywords_append_mode_keeps_defaults_and_adds_custom():
    keywords, custom = _build_keywords("MyCustomTerm,HBM", "append")

    assert custom == ["MyCustomTerm", "HBM"]
    assert "HBM" in keywords
    assert "MyCustomTerm" in keywords
    assert len(keywords) >= len(DEFAULT_AI_SEMICONDUCTOR_KEYWORDS)


def test_build_keywords_replace_mode_uses_only_custom():
    keywords, custom = _build_keywords("OnlyCustom", "replace")

    assert custom == ["OnlyCustom"]
    assert keywords == ["OnlyCustom"]
