from __future__ import annotations


def test_data_page_cache_ttl(monkeypatch):
    from ghtrader.control import views

    views._data_page_cache_clear()
    now = {"t": 100.0}
    monkeypatch.setattr(views.time, "time", lambda: now["t"])

    views._data_page_cache_set("k1", {"ok": True})
    assert views._data_page_cache_get("k1", ttl_s=5.0) == {"ok": True}

    now["t"] = 106.0
    assert views._data_page_cache_get("k1", ttl_s=5.0) is None
