from __future__ import annotations

import os
import sys
from pathlib import Path

from ghtrader.util.json_io import read_json, write_json_atomic
from ghtrader.util.time import now_iso


def _brokers_cache_path(*, runs_dir: Path) -> Path:
    return runs_dir / "control" / "cache" / "brokers.json"


def fallback_brokers() -> list[str]:
    # Keep this list tiny; it is only a UX fallback when network is unavailable.
    return ["T铜冠金源"]


def parse_brokers_from_shinny_html(html: str) -> list[str]:
    try:
        import html as html_mod
        import re

        # Quick-and-dirty tag strip to get searchable text.
        text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", "\n", str(html))
        text = re.sub(r"(?s)<[^>]+>", "\n", text)
        text = html_mod.unescape(text)

        # Broker IDs are typically like: "T铜冠金源", "H海通期货", "SIMNOW", etc.
        # We only extract the ones containing at least one CJK character.
        pat = re.compile(r"(?<![A-Za-z0-9_])([A-Z][A-Za-z0-9_]*[\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9_]*)(?![A-Za-z0-9_])")
        found = pat.findall(text)

        out: list[str] = []
        for s in found:
            s = str(s).strip()
            if not s or len(s) > 32:
                continue
            if s not in out:
                out.append(s)
        return out
    except Exception:
        return []


def fetch_shinny_brokers(*, timeout_s: float = 5.0) -> list[str]:
    url = "https://www.shinnytech.com/articles/reference/tqsdk-brokers"
    try:
        import urllib.request

        with urllib.request.urlopen(url, timeout=float(timeout_s)) as resp:  # nosec - expected public doc URL
            raw = resp.read()
        html = raw.decode("utf-8", errors="ignore")
        return parse_brokers_from_shinny_html(html)
    except Exception:
        return []


def get_supported_brokers(*, runs_dir: Path) -> tuple[list[str], str]:
    """
    Return (brokers, source) where source is one of: cache|fetched|fallback.
    """
    cache = _brokers_cache_path(runs_dir=runs_dir)
    cached = read_json(cache) if cache.exists() else None
    if isinstance(cached, dict) and isinstance(cached.get("brokers"), list) and cached.get("brokers"):
        brokers = [str(x) for x in cached["brokers"] if str(x).strip()]
        if brokers:
            return brokers, "cache"

    # Avoid network in tests.
    if os.environ.get("PYTEST_CURRENT_TEST") or "pytest" in sys.modules:
        return fallback_brokers(), "fallback"

    brokers = fetch_shinny_brokers()
    if brokers:
        write_json_atomic(
            cache,
            {
                "brokers": brokers,
                "fetched_at": now_iso(),
                "source_url": "https://www.shinnytech.com/articles/reference/tqsdk-brokers",
            },
        )
        return brokers, "fetched"

    return fallback_brokers(), "fallback"


__all__ = ["get_supported_brokers", "fetch_shinny_brokers", "parse_brokers_from_shinny_html", "fallback_brokers"]

