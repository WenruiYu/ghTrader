"""
Feature Store: Training-serving parity, TTL management, and feature lineage tracking.

This module implements PRD Section 5.4.0.4 Feature Store Architecture:
- FeatureDefinition: Metadata for each feature (lineage, TTL, versioning)
- FeatureRegistry: Centralized registry of all features with versioning
- OnlineFeatureStore: In-memory store for real-time serving with TTL
- Staleness detection and monitoring integration
"""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import structlog

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Feature Definition (metadata for each feature)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FeatureDefinition:
    """
    Metadata for a registered feature.

    Attributes:
        name: Feature/factor name (e.g., 'spread', 'return_10')
        version: Version number (auto-incremented on breaking changes)
        input_columns: List of raw tick columns this feature depends on
        output_dtype: Output data type ('float64', 'int64', etc.)
        lookback_ticks: Number of historical ticks required for computation
        ttl_seconds: Time-to-live for online serving (0 = no expiration)
        definition_hash: Hash of the feature definition for change detection
        created_at: When this feature version was registered
        deprecated_at: When this feature version was deprecated (None if active)
    """

    name: str
    version: int
    input_columns: tuple[str, ...]
    output_dtype: str
    lookback_ticks: int
    ttl_seconds: int
    definition_hash: str
    created_at: datetime
    deprecated_at: datetime | None = None

    def is_active(self) -> bool:
        """Check if this feature version is active (not deprecated)."""
        return self.deprecated_at is None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "version": self.version,
            "input_columns": list(self.input_columns),
            "output_dtype": self.output_dtype,
            "lookback_ticks": self.lookback_ticks,
            "ttl_seconds": self.ttl_seconds,
            "definition_hash": self.definition_hash,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "deprecated_at": self.deprecated_at.isoformat() if self.deprecated_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FeatureDefinition:
        """Create from dictionary."""
        return cls(
            name=str(data["name"]),
            version=int(data["version"]),
            input_columns=tuple(data.get("input_columns") or []),
            output_dtype=str(data.get("output_dtype", "float64")),
            lookback_ticks=int(data.get("lookback_ticks", 0)),
            ttl_seconds=int(data.get("ttl_seconds", 1800)),
            definition_hash=str(data.get("definition_hash", "")),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(timezone.utc),
            deprecated_at=datetime.fromisoformat(data["deprecated_at"]) if data.get("deprecated_at") else None,
        )


def compute_definition_hash(
    name: str,
    input_columns: tuple[str, ...],
    lookback_ticks: int,
    output_dtype: str = "float64",
) -> str:
    """
    Compute a stable hash for a feature definition.

    This hash changes when the feature's computation semantics change,
    triggering a version increment.
    """
    payload = json.dumps(
        {
            "name": name,
            "input_columns": sorted(input_columns),
            "lookback_ticks": lookback_ticks,
            "output_dtype": output_dtype,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Feature Registry
# ---------------------------------------------------------------------------


# Default TTL values by feature category (in seconds)
TTL_MICROSTRUCTURE = 30  # Spread, microprice - very short-lived
TTL_ROLLING_SHORT = 300  # 5-minute rolling features
TTL_ROLLING_LONG = 1800  # 30-minute rolling features (default)
TTL_DAILY = 86400  # Daily features


@dataclass
class FeatureRegistry:
    """
    Centralized registry of all features with versioning and lineage tracking.

    The registry maintains:
    - Active feature definitions with their metadata
    - Version history for each feature
    - Deprecation status

    Thread-safe for concurrent access.
    """

    _features: dict[str, dict[int, FeatureDefinition]] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def register(
        self,
        name: str,
        input_columns: tuple[str, ...] | list[str],
        lookback_ticks: int = 0,
        ttl_seconds: int = TTL_ROLLING_LONG,
        output_dtype: str = "float64",
    ) -> FeatureDefinition:
        """
        Register a new feature or update an existing one.

        If the definition_hash matches an existing version, returns that version.
        If the hash differs, creates a new version.

        Args:
            name: Feature name
            input_columns: List of input tick columns
            lookback_ticks: Required lookback window
            ttl_seconds: TTL for online serving
            output_dtype: Output data type

        Returns:
            The registered FeatureDefinition
        """
        input_cols = tuple(input_columns) if isinstance(input_columns, list) else input_columns
        definition_hash = compute_definition_hash(name, input_cols, lookback_ticks, output_dtype)

        with self._lock:
            if name not in self._features:
                self._features[name] = {}

            # Check if this exact definition already exists
            for version, feat_def in self._features[name].items():
                if feat_def.definition_hash == definition_hash and feat_def.is_active():
                    log.debug("feature_store.register_existing", name=name, version=version)
                    return feat_def

            # Create new version
            new_version = max(self._features[name].keys(), default=0) + 1
            now = datetime.now(timezone.utc)

            feat_def = FeatureDefinition(
                name=name,
                version=new_version,
                input_columns=input_cols,
                output_dtype=output_dtype,
                lookback_ticks=lookback_ticks,
                ttl_seconds=ttl_seconds,
                definition_hash=definition_hash,
                created_at=now,
                deprecated_at=None,
            )

            self._features[name][new_version] = feat_def
            log.info("feature_store.registered", name=name, version=new_version, hash=definition_hash)
            return feat_def

    def get(self, name: str, version: int | None = None) -> FeatureDefinition | None:
        """
        Get a feature definition by name and optional version.

        Args:
            name: Feature name
            version: Specific version (None = latest active)

        Returns:
            FeatureDefinition or None if not found
        """
        with self._lock:
            if name not in self._features:
                return None

            versions = self._features[name]
            if not versions:
                return None

            if version is not None:
                return versions.get(version)

            # Return latest active version
            active_versions = [(v, fd) for v, fd in versions.items() if fd.is_active()]
            if not active_versions:
                return None
            return max(active_versions, key=lambda x: x[0])[1]

    def list_active(self) -> list[FeatureDefinition]:
        """List all active (non-deprecated) feature definitions."""
        with self._lock:
            result = []
            for versions in self._features.values():
                for feat_def in versions.values():
                    if feat_def.is_active():
                        result.append(feat_def)
            return sorted(result, key=lambda f: (f.name, f.version))

    def list_all(self) -> list[FeatureDefinition]:
        """List all feature definitions including deprecated ones."""
        with self._lock:
            result = []
            for versions in self._features.values():
                for feat_def in versions.values():
                    result.append(feat_def)
            return sorted(result, key=lambda f: (f.name, f.version))

    def deprecate(self, name: str, version: int) -> bool:
        """
        Deprecate a specific feature version.

        Args:
            name: Feature name
            version: Version to deprecate

        Returns:
            True if deprecated, False if not found
        """
        with self._lock:
            if name not in self._features:
                return False
            if version not in self._features[name]:
                return False

            old_def = self._features[name][version]
            if old_def.deprecated_at is not None:
                return False  # Already deprecated

            # Create new definition with deprecation timestamp
            new_def = FeatureDefinition(
                name=old_def.name,
                version=old_def.version,
                input_columns=old_def.input_columns,
                output_dtype=old_def.output_dtype,
                lookback_ticks=old_def.lookback_ticks,
                ttl_seconds=old_def.ttl_seconds,
                definition_hash=old_def.definition_hash,
                created_at=old_def.created_at,
                deprecated_at=datetime.now(timezone.utc),
            )

            self._features[name][version] = new_def
            log.info("feature_store.deprecated", name=name, version=version)
            return True

    def clear(self) -> None:
        """Clear all registered features (for testing)."""
        with self._lock:
            self._features.clear()

    def sync_to_questdb(self, cfg: Any) -> int:
        """
        Sync all feature definitions to QuestDB registry table.

        Args:
            cfg: QuestDBQueryConfig

        Returns:
            Number of features synced
        """
        from ghtrader.questdb.features_labels import sync_feature_registry

        features = self.list_all()
        sync_feature_registry(cfg=cfg, features=features)
        log.info("feature_store.synced_to_questdb", count=len(features))
        return len(features)


# ---------------------------------------------------------------------------
# Online Feature Store (in-memory with TTL)
# ---------------------------------------------------------------------------


@dataclass
class FeatureValue:
    """A feature value with timestamp for TTL checking."""

    value: float
    timestamp: datetime
    ttl_seconds: int = TTL_ROLLING_LONG


@dataclass
class OnlineFeatureStore:
    """
    In-memory feature store for real-time serving with TTL management.

    Provides sub-10ms retrieval latency for online inference.

    Key: (symbol, feature_name) -> FeatureValue (latest value + timestamp)
    """

    _store: dict[tuple[str, str], FeatureValue] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _registry: FeatureRegistry | None = None

    ttl_default_seconds: int = TTL_ROLLING_LONG

    def set_registry(self, registry: FeatureRegistry) -> None:
        """Set the feature registry for TTL lookups."""
        self._registry = registry

    def put(
        self,
        symbol: str,
        feature_name: str,
        value: float,
        ts: datetime | None = None,
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Store a feature value.

        Args:
            symbol: Instrument symbol
            feature_name: Feature name
            value: Feature value
            ts: Timestamp (default: now)
            ttl_seconds: TTL override (default: from registry or default)
        """
        if ts is None:
            ts = datetime.now(timezone.utc)

        # Get TTL from registry if available
        if ttl_seconds is None:
            if self._registry:
                feat_def = self._registry.get(feature_name)
                ttl_seconds = feat_def.ttl_seconds if feat_def else self.ttl_default_seconds
            else:
                ttl_seconds = self.ttl_default_seconds

        key = (symbol, feature_name)
        with self._lock:
            self._store[key] = FeatureValue(value=value, timestamp=ts, ttl_seconds=ttl_seconds)

    def get(self, symbol: str, feature_name: str) -> tuple[float, datetime] | None:
        """
        Get a feature value if it exists and is not stale.

        Args:
            symbol: Instrument symbol
            feature_name: Feature name

        Returns:
            (value, timestamp) tuple or None if not found or stale
        """
        key = (symbol, feature_name)
        with self._lock:
            fv = self._store.get(key)
            if fv is None:
                return None

            # Check staleness
            if self._is_stale_internal(fv):
                return None

            return (fv.value, fv.timestamp)

    def get_batch(
        self,
        symbol: str,
        features: list[str],
        include_stale: bool = False,
    ) -> dict[str, float | None]:
        """
        Get multiple feature values for a symbol.

        Args:
            symbol: Instrument symbol
            features: List of feature names
            include_stale: If True, return stale values too

        Returns:
            Dict of feature_name -> value (None if not found or stale)
        """
        result: dict[str, float | None] = {}
        now = datetime.now(timezone.utc)

        with self._lock:
            for feature_name in features:
                key = (symbol, feature_name)
                fv = self._store.get(key)

                if fv is None:
                    result[feature_name] = None
                elif not include_stale and self._is_stale_internal(fv, now):
                    result[feature_name] = None
                else:
                    result[feature_name] = fv.value

        return result

    def is_stale(self, symbol: str, feature_name: str) -> bool:
        """
        Check if a feature value is stale (past TTL).

        Args:
            symbol: Instrument symbol
            feature_name: Feature name

        Returns:
            True if stale or not found
        """
        key = (symbol, feature_name)
        with self._lock:
            fv = self._store.get(key)
            if fv is None:
                return True
            return self._is_stale_internal(fv)

    def _is_stale_internal(self, fv: FeatureValue, now: datetime | None = None) -> bool:
        """Check if a FeatureValue is stale (internal, no lock)."""
        if fv.ttl_seconds <= 0:
            return False  # No TTL = never stale

        if now is None:
            now = datetime.now(timezone.utc)

        age_seconds = (now - fv.timestamp).total_seconds()
        return age_seconds > fv.ttl_seconds

    def clear_symbol(self, symbol: str) -> int:
        """
        Clear all features for a symbol.

        Useful when switching underlying contracts or resetting state.

        Args:
            symbol: Instrument symbol

        Returns:
            Number of features cleared
        """
        with self._lock:
            keys_to_remove = [k for k in self._store.keys() if k[0] == symbol]
            for key in keys_to_remove:
                del self._store[key]
            return len(keys_to_remove)

    def clear_all(self) -> int:
        """
        Clear all stored features.

        Returns:
            Number of features cleared
        """
        with self._lock:
            count = len(self._store)
            self._store.clear()
            return count

    def get_stats(self) -> dict[str, Any]:
        """
        Get statistics about the online store.

        Returns:
            Dict with count, symbols, stale count, etc.
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            symbols = set(k[0] for k in self._store.keys())
            features = set(k[1] for k in self._store.keys())
            stale_count = sum(1 for fv in self._store.values() if self._is_stale_internal(fv, now))

            return {
                "total_entries": len(self._store),
                "symbols_count": len(symbols),
                "features_count": len(features),
                "stale_count": stale_count,
                "fresh_count": len(self._store) - stale_count,
            }


# ---------------------------------------------------------------------------
# Staleness Detection Result
# ---------------------------------------------------------------------------


@dataclass
class StalenessResult:
    """Result of staleness check with details."""

    symbol: str
    feature_name: str
    is_stale: bool
    last_update: datetime | None
    age_seconds: float | None
    ttl_seconds: int
    reason: str | None = None


def check_staleness(
    store: OnlineFeatureStore,
    symbol: str,
    feature_name: str,
    registry: FeatureRegistry | None = None,
) -> StalenessResult:
    """
    Check staleness of a feature with detailed result.

    Args:
        store: OnlineFeatureStore instance
        symbol: Instrument symbol
        feature_name: Feature name
        registry: Optional FeatureRegistry for TTL lookup

    Returns:
        StalenessResult with full details
    """
    # Get TTL from registry if available
    ttl_seconds = TTL_ROLLING_LONG
    if registry:
        feat_def = registry.get(feature_name)
        if feat_def:
            ttl_seconds = feat_def.ttl_seconds

    # Check the store
    key = (symbol, feature_name)
    with store._lock:
        fv = store._store.get(key)

        if fv is None:
            return StalenessResult(
                symbol=symbol,
                feature_name=feature_name,
                is_stale=True,
                last_update=None,
                age_seconds=None,
                ttl_seconds=ttl_seconds,
                reason="not_found",
            )

        now = datetime.now(timezone.utc)
        age_seconds = (now - fv.timestamp).total_seconds()
        is_stale = age_seconds > fv.ttl_seconds if fv.ttl_seconds > 0 else False

        return StalenessResult(
            symbol=symbol,
            feature_name=feature_name,
            is_stale=is_stale,
            last_update=fv.timestamp,
            age_seconds=age_seconds,
            ttl_seconds=fv.ttl_seconds,
            reason="ttl_exceeded" if is_stale else None,
        )


# ---------------------------------------------------------------------------
# Global default instances
# ---------------------------------------------------------------------------

# Global feature registry (singleton for the process)
_global_registry: FeatureRegistry | None = None
_global_online_store: OnlineFeatureStore | None = None


def get_feature_registry() -> FeatureRegistry:
    """Get the global feature registry singleton."""
    global _global_registry
    if _global_registry is None:
        _global_registry = FeatureRegistry()
    return _global_registry


def get_online_feature_store() -> OnlineFeatureStore:
    """Get the global online feature store singleton."""
    global _global_online_store
    if _global_online_store is None:
        _global_online_store = OnlineFeatureStore()
        _global_online_store.set_registry(get_feature_registry())
    return _global_online_store


# ---------------------------------------------------------------------------
# Staleness Monitoring (integration point for drift detection - PRD 5.14)
# ---------------------------------------------------------------------------


@dataclass
class StalenessEvent:
    """Event emitted when a feature becomes stale or recovers."""

    timestamp: datetime
    symbol: str
    feature_name: str
    event_type: str  # "stale" or "recovered"
    age_seconds: float | None
    ttl_seconds: int
    details: dict[str, Any] = field(default_factory=dict)


# Type alias for staleness event handlers
StalenessHandler = Callable[[StalenessEvent], None]


class StalenessMonitor:
    """
    Monitor for feature staleness with configurable alerting.

    This class provides the integration point for drift detection (PRD 5.14):
    - Tracks staleness events across symbols and features
    - Emits events when features become stale or recover
    - Provides aggregate statistics for dashboard display
    - Supports configurable handlers for integration with alerting systems

    Usage:
        monitor = StalenessMonitor(store, registry)
        monitor.add_handler(my_alert_handler)
        monitor.check_all("SHFE.cu2501")  # Check staleness, emit events
    """

    def __init__(
        self,
        store: OnlineFeatureStore,
        registry: FeatureRegistry | None = None,
    ):
        self._store = store
        self._registry = registry or get_feature_registry()
        self._handlers: list[StalenessHandler] = []
        self._last_state: dict[tuple[str, str], bool] = {}  # (symbol, feature) -> was_stale
        self._lock = threading.Lock()
        self._event_history: list[StalenessEvent] = []
        self._max_history = 1000

    def add_handler(self, handler: StalenessHandler) -> None:
        """Add a handler to be called on staleness events."""
        with self._lock:
            self._handlers.append(handler)

    def remove_handler(self, handler: StalenessHandler) -> None:
        """Remove a handler."""
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)

    def check_feature(
        self,
        symbol: str,
        feature_name: str,
    ) -> StalenessResult:
        """
        Check staleness of a single feature and emit event if state changed.

        Args:
            symbol: Instrument symbol
            feature_name: Feature name

        Returns:
            StalenessResult with current state
        """
        result = check_staleness(self._store, symbol, feature_name, self._registry)

        key = (symbol, feature_name)
        with self._lock:
            was_stale = self._last_state.get(key)
            self._last_state[key] = result.is_stale

            # Emit event on state change
            if was_stale is not None and was_stale != result.is_stale:
                event = StalenessEvent(
                    timestamp=datetime.now(timezone.utc),
                    symbol=symbol,
                    feature_name=feature_name,
                    event_type="stale" if result.is_stale else "recovered",
                    age_seconds=result.age_seconds,
                    ttl_seconds=result.ttl_seconds,
                    details={"reason": result.reason} if result.reason else {},
                )
                self._emit_event(event)

        return result

    def check_all(self, symbol: str) -> dict[str, StalenessResult]:
        """
        Check staleness of all registered features for a symbol.

        Args:
            symbol: Instrument symbol

        Returns:
            Dict of feature_name -> StalenessResult
        """
        results: dict[str, StalenessResult] = {}
        active_features = self._registry.list_active()

        for feat_def in active_features:
            results[feat_def.name] = self.check_feature(symbol, feat_def.name)

        return results

    def get_stale_features(self, symbol: str) -> list[StalenessResult]:
        """
        Get list of stale features for a symbol.

        Args:
            symbol: Instrument symbol

        Returns:
            List of StalenessResults for stale features only
        """
        all_results = self.check_all(symbol)
        return [r for r in all_results.values() if r.is_stale]

    def get_staleness_summary(self, symbol: str) -> dict[str, Any]:
        """
        Get a summary of staleness state for a symbol.

        Useful for dashboard display and drift detection.

        Args:
            symbol: Instrument symbol

        Returns:
            Summary dict with counts and lists
        """
        all_results = self.check_all(symbol)

        stale = [r for r in all_results.values() if r.is_stale]
        fresh = [r for r in all_results.values() if not r.is_stale]

        return {
            "symbol": symbol,
            "total_features": len(all_results),
            "stale_count": len(stale),
            "fresh_count": len(fresh),
            "stale_ratio": len(stale) / len(all_results) if all_results else 0.0,
            "stale_features": [r.feature_name for r in stale],
            "oldest_age_seconds": max((r.age_seconds for r in stale if r.age_seconds), default=None),
        }

    def get_event_history(self, limit: int = 100) -> list[StalenessEvent]:
        """Get recent staleness events."""
        with self._lock:
            return list(self._event_history[-limit:])

    def _emit_event(self, event: StalenessEvent) -> None:
        """Emit event to all handlers and record in history."""
        # Record in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        # Notify handlers
        handlers = list(self._handlers)  # Copy to avoid holding lock during callbacks

        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                log.warning("staleness_monitor.handler_failed", error=str(e))


def log_staleness_handler(event: StalenessEvent) -> None:
    """
    Default handler that logs staleness events.

    This can be used as a template for custom handlers.
    """
    log.info(
        "staleness_event",
        event_type=event.event_type,
        symbol=event.symbol,
        feature=event.feature_name,
        age_seconds=event.age_seconds,
        ttl_seconds=event.ttl_seconds,
    )


# Global staleness monitor singleton
_global_staleness_monitor: StalenessMonitor | None = None


def get_staleness_monitor() -> StalenessMonitor:
    """Get the global staleness monitor singleton."""
    global _global_staleness_monitor
    if _global_staleness_monitor is None:
        _global_staleness_monitor = StalenessMonitor(
            store=get_online_feature_store(),
            registry=get_feature_registry(),
        )
        # Add default logging handler
        _global_staleness_monitor.add_handler(log_staleness_handler)
    return _global_staleness_monitor
