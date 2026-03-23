from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TypeAlias

from expidite_rpi.core import configuration as root_cfg

logger = root_cfg.setup_logger("expidite")

JSONPrimitive: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONPrimitive | list["JSONValue"] | dict[str, "JSONValue"]


@dataclass(frozen=True)
class SharedStateEntry:
    value: JSONValue
    version: int
    updated_at: datetime
    expires_at: datetime | None = None


class SharedState:
    """Thread-safe in-memory key/value store for Sensor and DataProcessor subclasses.

    Intended for low-volume control-plane state (flags, thresholds, status), not high-volume payload data.
    """

    _instance: SharedState | None = None
    _instance_lock = threading.Lock()

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._values: dict[str, SharedStateEntry] = {}

    @staticmethod
    def get_instance() -> SharedState:
        with SharedState._instance_lock:
            if SharedState._instance is None:
                SharedState._instance = SharedState()
        return SharedState._instance

    def set(self, key: str, value: JSONValue, ttl_seconds: float | None = None) -> int:
        """Set a value for key and return the new key version."""
        self._validate_key(key)
        self._validate_value(value)

        expires_at: datetime | None = None
        if ttl_seconds is not None:
            if ttl_seconds <= 0:
                msg = f"ttl_seconds must be > 0; received {ttl_seconds}"
                raise ValueError(msg)
            expires_at = datetime.now(tz=UTC) + timedelta(seconds=ttl_seconds)

        with self._lock:
            self._purge_expired_locked()
            current = self._values.get(key)
            version = 1 if current is None else current.version + 1
            self._values[key] = SharedStateEntry(
                value=value,
                version=version,
                updated_at=datetime.now(tz=UTC),
                expires_at=expires_at,
            )
            return version

    def get(self, key: str, default: JSONValue | None = None) -> JSONValue | None:
        """Get the value for key, or default if absent or expired."""
        entry = self.get_entry(key)
        if entry is None:
            return default
        return entry.value

    def get_entry(self, key: str) -> SharedStateEntry | None:
        """Get entry metadata for key, or None if absent or expired."""
        self._validate_key(key)
        with self._lock:
            self._purge_expired_locked()
            return self._values.get(key)

    def delete(self, key: str) -> bool:
        """Delete key and return True if the key existed."""
        self._validate_key(key)
        with self._lock:
            self._purge_expired_locked()
            return self._values.pop(key, None) is not None

    def list_keys(self, prefix: str | None = None) -> list[str]:
        """List active keys, optionally filtered by a prefix."""
        with self._lock:
            self._purge_expired_locked()
            keys = list(self._values)
        if prefix is None:
            return sorted(keys)
        return sorted([k for k in keys if k.startswith(prefix)])

    def clear(self) -> None:
        """Clear all keys. Intended for tests and controlled resets."""
        with self._lock:
            self._values.clear()

    def _purge_expired_locked(self) -> None:
        now = datetime.now(tz=UTC)
        expired_keys = [
            key
            for key, entry in self._values.items()
            if entry.expires_at is not None and entry.expires_at <= now
        ]
        for key in expired_keys:
            del self._values[key]

    @staticmethod
    def _validate_key(key: str) -> None:
        if not key or not key.strip():
            raise ValueError("SharedState key must be a non-empty string")

    @staticmethod
    def _validate_value(value: JSONValue) -> None:
        try:
            json.dumps(value)
        except (TypeError, ValueError) as ex:
            msg = "SharedState only accepts JSON-serializable values"
            raise ValueError(msg) from ex
