from __future__ import annotations

import time

from ghtrader.control.cache import TTLCacheMap, TTLCacheSlot


def test_ttl_cache_slot_key_and_expiry() -> None:
    slot = TTLCacheSlot()
    now = time.time()
    slot.set({"x": 1}, key="a", now=now)
    assert slot.get(ttl_s=5.0, key="a", now=now + 1.0) == {"x": 1}
    assert slot.get(ttl_s=5.0, key="b", now=now + 1.0) is None
    assert slot.get(ttl_s=1.0, key="a", now=now + 2.0) is None


def test_ttl_cache_map_set_get_and_clear() -> None:
    cache = TTLCacheMap()
    now = time.time()
    cache.set("k1", {"ok": True}, now=now)
    assert cache.get("k1", ttl_s=5.0, now=now + 0.5) == {"ok": True}
    assert cache.get("k1", ttl_s=0.1, now=now + 0.2) is None
    cache.set("k2", 42, now=now)
    cache.clear()
    assert cache.get("k2", ttl_s=5.0, now=now + 0.1) is None

