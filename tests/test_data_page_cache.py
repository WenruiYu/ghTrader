from __future__ import annotations


def test_data_page_cache_ttl(monkeypatch):
    from ghtrader.control import views_data_page as vdp

    vdp.clear_data_page_cache()
    now = {"t": 100.0}
    monkeypatch.setattr(vdp.time, "time", lambda: now["t"])

    vdp._data_page_cache_set("k1", {"ok": True})
    assert vdp._data_page_cache_get("k1", ttl_s=5.0) == {"ok": True}

    now["t"] = 106.0
    assert vdp._data_page_cache_get("k1", ttl_s=5.0) is None
