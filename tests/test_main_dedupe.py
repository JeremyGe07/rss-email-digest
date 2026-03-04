from src.main import (
    _filter_seen_posts,
    _post_fingerprint,
    _prune_seen_posts,
    _update_seen_posts,
)
from datetime import datetime, timedelta, timezone


def test_filter_seen_posts_removes_already_sent_and_updates_status():
    feed_results = [
        {
            "name": "Feed A",
            "status": "success",
            "posts": [
                {"title": "A1", "link": "https://example.com/a1", "excerpt": "x"},
                {"title": "A2", "link": "https://example.com/a2", "excerpt": "x"},
            ],
        }
    ]
    seen = {"link::https://example.com/a1": datetime.now(timezone.utc).isoformat()}

    filtered, total_before, removed = _filter_seen_posts(feed_results, seen)

    assert total_before == 2
    assert removed == 1
    assert len(filtered[0]["posts"]) == 1
    assert filtered[0]["posts"][0]["title"] == "A2"


def test_update_seen_posts_records_fingerprints():
    feed_results = [
        {
            "name": "Feed A",
            "posts": [
                {"title": "A1", "link": "https://example.com/a1", "excerpt": "x"},
            ],
        }
    ]
    seen = {}
    updated = _update_seen_posts(feed_results, seen)

    assert "link::https://example.com/a1" in updated


def test_prune_seen_posts_drops_old_entries():
    now = datetime.now(timezone.utc)
    seen = {
        "new": now.isoformat(),
        "old": (now - timedelta(days=40)).isoformat(),
    }
    pruned = _prune_seen_posts(seen, ttl_days=30)

    assert "new" in pruned
    assert "old" not in pruned


def test_post_fingerprint_falls_back_to_hash_without_link():
    fp = _post_fingerprint("Feed A", {"title": "T", "excerpt": "E", "link": ""})
    assert fp.startswith("hash::")
