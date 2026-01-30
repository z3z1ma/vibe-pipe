"""
Result caching with TTL support for Vibe Piper.

This module provides:
- Thread-safe caching with TTL expiration
- Cache key generation from inputs + code hash
- Cache invalidation logic
- Memory and disk-based cache backends
"""

import hashlib
import inspect
import json
import logging
import pickle
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Type Variables
# =============================================================================

P = ParamSpec("P")
T = TypeVar("T")

# =============================================================================
# Cache Data Structures
# =============================================================================


@dataclass(frozen=True)
class CacheKey:
    """
    Unique cache key for an asset execution.

    Attributes:
        asset_name: Name of the asset
        inputs_hash: Hash of input data
        code_hash: Hash of the function code
        metadata: Additional metadata (e.g., config hashes)
    """

    asset_name: str
    inputs_hash: str
    code_hash: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """String representation of cache key."""
        return f"{self.asset_name}:{self.inputs_hash}:{self.code_hash}"

    def to_string(self) -> str:
        """Convert cache key to string for storage."""
        return self.__str__()


@dataclass(frozen=True)
class CacheEntry:
    """
    Cached value with metadata.

    Attributes:
        key: Cache key for this entry
        value: Cached value
        created_at: When the entry was created
        expires_at: When the entry expires
        hit_count: Number of times this entry was accessed
        size_bytes: Approximate size in memory
    """

    key: CacheKey
    value: Any
    created_at: datetime
    expires_at: datetime | None
    hit_count: int = 0
    size_bytes: int | None = None

    @property
    def is_expired(self) -> bool:
        """Check if cache entry has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() >= self.expires_at

    @property
    def age_seconds(self) -> float:
        """Age of cache entry in seconds."""
        return (datetime.utcnow() - self.created_at).total_seconds()


# =============================================================================
# Cache Backends
# =============================================================================


class CacheBackend(ABC):
    """Abstract base class for cache backends."""

    @abstractmethod
    def get(self, key: CacheKey) -> CacheEntry | None:
        """Get cache entry by key."""
        ...

    @abstractmethod
    def set(self, key: CacheKey, value: Any, ttl: int | None = None) -> None:
        """Set cache entry with optional TTL."""
        ...

    @abstractmethod
    def delete(self, key: CacheKey) -> bool:
        """Delete cache entry by key."""
        ...

    @abstractmethod
    def clear(self) -> None:
        """Clear all cache entries."""
        ...

    @abstractmethod
    def cleanup_expired(self) -> int:
        """Remove expired entries and return count removed."""
        ...

    @abstractmethod
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        ...


class MemoryCacheBackend(CacheBackend):
    """
    In-memory cache backend.

    Thread-safe cache with TTL support.

    Attributes:
        max_size: Maximum number of entries to store
        cleanup_interval: How often to clean up expired entries
    """

    def __init__(self, max_size: int = 1000, cleanup_interval: int = 300) -> None:
        """
        Initialize memory cache backend.

        Args:
            max_size: Maximum number of entries to store
            cleanup_interval: How often to clean up expired entries (seconds)
        """
        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._last_cleanup = time.time()

    def get(self, key: CacheKey) -> CacheEntry | None:
        """
        Get cache entry by key.

        Args:
            key: Cache key to retrieve

        Returns:
            CacheEntry if found and not expired, None otherwise
        """
        with self._lock:
            # Periodic cleanup
            self._maybe_cleanup()

            entry = self._cache.get(key.to_string())

            if entry is None:
                self._misses += 1
                return None

            # Check expiration
            if entry.is_expired:
                # Remove expired entry
                del self._cache[key.to_string()]
                self._misses += 1
                logger.debug(f"Cache expired for {key.asset_name}")
                return None

            # Update hit count
            updated_entry = CacheEntry(
                key=entry.key,
                value=entry.value,
                created_at=entry.created_at,
                expires_at=entry.expires_at,
                hit_count=entry.hit_count + 1,
                size_bytes=entry.size_bytes,
            )
            self._cache[key.to_string()] = updated_entry
            self._hits += 1

            logger.debug(f"Cache hit for {key.asset_name}")
            return updated_entry

    def set(self, key: CacheKey, value: Any, ttl: int | None = None) -> None:
        """
        Set cache entry with optional TTL.

        Args:
            key: Cache key to store
            value: Value to cache
            ttl: Time-to-live in seconds (None = no expiration)
        """
        with self._lock:
            # Enforce max size
            if len(self._cache) >= self.max_size:
                # Simple eviction: remove oldest entries
                self._evict_oldest()

            # Calculate expiration
            expires_at = None
            if ttl is not None and ttl > 0:
                expires_at = datetime.utcnow() + timedelta(seconds=ttl)

            # Estimate size
            size_bytes = self._estimate_size(value)

            # Create entry
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.utcnow(),
                expires_at=expires_at,
                hit_count=0,
                size_bytes=size_bytes,
            )

            self._cache[key.to_string()] = entry
            logger.debug(f"Cached {key.asset_name} with TTL={ttl}")

    def delete(self, key: CacheKey) -> bool:
        """
        Delete cache entry by key.

        Args:
            key: Cache key to delete

        Returns:
            True if entry was deleted, False if not found
        """
        with self._lock:
            key_str = key.to_string()
            if key_str in self._cache:
                del self._cache[key_str]
                logger.debug(f"Cache invalidated for {key.asset_name}")
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._hits = 0
            self._misses = 0
            self._evictions = 0
            logger.info(f"Cleared cache ({count} entries)")

    def cleanup_expired(self) -> int:
        """
        Remove expired entries.

        Returns:
            Number of entries removed
        """
        with self._lock:
            now = datetime.utcnow()
            expired_keys = [
                k
                for k, v in self._cache.items()
                if v.expires_at is not None and now >= v.expires_at
            ]

            for key in expired_keys:
                del self._cache[key]

            if expired_keys:
                logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

            return len(expired_keys)

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0

            total_size = sum(v.size_bytes or 0 for v in self._cache.values())
            avg_hit_count = (
                sum(v.hit_count for v in self._cache.values()) / len(self._cache)
                if self._cache
                else 0
            )

            return {
                "entries": len(self._cache),
                "max_size": self.max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "evictions": self._evictions,
                "total_size_bytes": total_size,
                "avg_hit_count": avg_hit_count,
            }

    def _evict_oldest(self) -> None:
        """Evict oldest entries to free space."""
        if not self._cache:
            return

        # Sort by creation time and remove oldest 10%
        sorted_entries = sorted(self._cache.values(), key=lambda e: e.created_at)
        to_remove = max(1, len(sorted_entries) // 10)

        for entry in sorted_entries[:to_remove]:
            del self._cache[entry.key.to_string()]
            self._evictions += 1

        logger.debug(f"Evicted {to_remove} oldest cache entries")

    def _maybe_cleanup(self) -> None:
        """Run cleanup if interval has passed."""
        now = time.time()
        if now - self._last_cleanup >= self.cleanup_interval:
            self.cleanup_expired()
            self._last_cleanup = now

    @staticmethod
    def _estimate_size(value: Any) -> int:
        """
        Estimate memory size of a value.

        Args:
            value: Value to estimate size

        Returns:
            Estimated size in bytes
        """
        try:
            return len(pickle.dumps(value))
        except Exception:
            # Fallback: use string representation
            return len(str(value))


class DiskCacheBackend(CacheBackend):
    """
    Disk-based cache backend.

    Stores cache entries as files on disk.

    Attributes:
        cache_dir: Directory for cache files
        max_size_mb: Maximum total size in MB
    """

    def __init__(self, cache_dir: Path | str, max_size_mb: int = 1024) -> None:
        """
        Initialize disk cache backend.

        Args:
            cache_dir: Directory for cache files
            max_size_mb: Maximum total size in MB
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_size_mb = max_size_mb
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def _get_cache_path(self, key: CacheKey) -> Path:
        """Get file path for cache entry."""
        # Hash the key string to create a safe filename
        key_hash = hashlib.sha256(key.to_string().encode()).hexdigest()
        return self.cache_dir / f"{key_hash}.cache"

    def get(self, key: CacheKey) -> CacheEntry | None:
        """
        Get cache entry by key.

        Args:
            key: Cache key to retrieve

        Returns:
            CacheEntry if found and not expired, None otherwise
        """
        cache_path = self._get_cache_path(key)

        if not cache_path.exists():
            self._misses += 1
            return None

        try:
            with open(cache_path, "rb") as f:
                data = pickle.load(f)

            # Reconstruct CacheEntry
            entry = CacheEntry(
                key=CacheKey(**data["key"]),
                value=data["value"],
                created_at=datetime.fromisoformat(data["created_at"]),
                expires_at=(
                    datetime.fromisoformat(data["expires_at"]) if data["expires_at"] else None
                ),
                hit_count=data.get("hit_count", 0) + 1,
                size_bytes=data.get("size_bytes"),
            )

            # Check expiration
            if entry.is_expired:
                self._misses += 1
                cache_path.unlink(missing_ok=True)
                logger.debug(f"Cache expired for {key.asset_name}")
                return None

            self._hits += 1
            return entry

        except (pickle.PickleError, KeyError, ValueError) as e:
            logger.warning(f"Failed to load cache entry: {e}")
            self._misses += 1
            return None

    def set(self, key: CacheKey, value: Any, ttl: int | None = None) -> None:
        """
        Set cache entry with optional TTL.

        Args:
            key: Cache key to store
            value: Value to cache
            ttl: Time-to-live in seconds (None = no expiration)
        """
        cache_path = self._get_cache_path(key)

        # Calculate expiration
        expires_at = None
        if ttl is not None and ttl > 0:
            expires_at = datetime.utcnow() + timedelta(seconds=ttl)

        # Prepare data
        data = {
            "key": {
                "asset_name": key.asset_name,
                "inputs_hash": key.inputs_hash,
                "code_hash": key.code_hash,
                "metadata": key.metadata,
            },
            "value": value,
            "created_at": datetime.utcnow().isoformat(),
            "expires_at": expires_at.isoformat() if expires_at else None,
            "hit_count": 0,
        }

        try:
            # Estimate size
            data_bytes = pickle.dumps(data)
            data["size_bytes"] = len(data_bytes)

            # Check max size
            self._maybe_evict()

            # Write to disk
            temp_path = cache_path.with_suffix(".tmp")
            with open(temp_path, "wb") as f:
                pickle.dump(data, f)

            # Atomic rename
            temp_path.replace(cache_path)

            logger.debug(f"Cached {key.asset_name} to disk with TTL={ttl}")

        except (pickle.PickleError, OSError) as e:
            logger.warning(f"Failed to write cache entry: {e}")

    def delete(self, key: CacheKey) -> bool:
        """
        Delete cache entry by key.

        Args:
            key: Cache key to delete

        Returns:
            True if entry was deleted, False if not found
        """
        cache_path = self._get_cache_path(key)

        if cache_path.exists():
            cache_path.unlink(missing_ok=True)
            logger.debug(f"Cache invalidated for {key.asset_name}")
            return True

        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            count = 0
            for file in self.cache_dir.glob("*.cache"):
                file.unlink(missing_ok=True)
                count += 1

            self._hits = 0
            self._misses = 0
            self._evictions = 0

            logger.info(f"Cleared disk cache ({count} entries)")

    def cleanup_expired(self) -> int:
        """
        Remove expired entries.

        Returns:
            Number of entries removed
        """
        count = 0
        now = datetime.utcnow()

        for cache_path in self.cache_dir.glob("*.cache"):
            try:
                with open(cache_path, "rb") as f:
                    data = pickle.load(f)

                expires_at = data.get("expires_at")
                if expires_at:
                    expires = datetime.fromisoformat(expires_at)
                    if now >= expires:
                        cache_path.unlink(missing_ok=True)
                        count += 1

            except Exception:
                # Corrupted file, remove it
                cache_path.unlink(missing_ok=True)

        if count > 0:
            logger.debug(f"Cleaned up {count} expired cache entries from disk")

        return count

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0

            # Count entries and calculate total size
            entries = 0
            total_size = 0
            for cache_path in self.cache_dir.glob("*.cache"):
                entries += 1
                total_size += cache_path.stat().st_size

            return {
                "entries": entries,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "evictions": self._evictions,
                "total_size_bytes": total_size,
                "total_size_mb": total_size / (1024 * 1024),
            }

    def _maybe_evict(self) -> None:
        """Evict oldest entries if over max size."""
        total_size = sum(f.stat().st_size for f in self.cache_dir.glob("*.cache"))
        max_bytes = self.max_size_mb * 1024 * 1024

        if total_size < max_bytes:
            return

        # Sort by modification time and remove oldest 10%
        files = sorted(self.cache_dir.glob("*.cache"), key=lambda f: f.stat().st_mtime)
        to_remove = max(1, len(files) // 10)

        for file in files[:to_remove]:
            file.unlink(missing_ok=True)
            self._evictions += 1

        logger.debug(f"Evicted {to_remove} oldest cache entries from disk")


# =============================================================================
# Cache Manager
# =============================================================================


class CacheManager:
    """
    High-level cache manager for asset execution.

    Provides:
    - Cache key generation from inputs and code
    - Automatic cache invalidation
    - Statistics tracking

    Attributes:
        backend: Cache backend to use
        enabled: Whether caching is enabled
    """

    def __init__(
        self,
        backend: CacheBackend | None = None,
        enabled: bool = True,
    ) -> None:
        """
        Initialize cache manager.

        Args:
            backend: Cache backend to use (defaults to MemoryCacheBackend)
            enabled: Whether caching is enabled
        """
        self.backend = backend or MemoryCacheBackend()
        self.enabled = enabled
        self._code_hashes: dict[str, str] = {}

    def compute_cache_key(
        self,
        asset_name: str,
        inputs: Any,
        fn: Callable[P, T] | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> CacheKey:
        """
        Compute cache key from asset name, inputs, and function.

        Args:
            asset_name: Name of the asset
            inputs: Input data to hash
            fn: Optional function to hash
            config: Optional config to include in hash

        Returns:
            CacheKey for this execution
        """
        # Hash inputs
        inputs_hash = self._hash_value(inputs)

        # Hash code if provided
        code_hash = ""
        if fn is not None:
            if fn in self._code_hashes:
                code_hash = self._code_hashes[fn]
            else:
                code_hash = self._hash_function(fn)
                self._code_hashes[fn] = code_hash

        # Hash config if provided
        config_hash = ""
        if config:
            config_hash = self._hash_value(config)

        metadata = {}
        if config_hash:
            metadata["config_hash"] = config_hash

        return CacheKey(
            asset_name=asset_name,
            inputs_hash=inputs_hash,
            code_hash=code_hash,
            metadata=metadata,
        )

    def get(self, asset_name: str, inputs: Any, fn: Callable[P, T] | None = None) -> Any | None:
        """
        Get cached result.

        Args:
            asset_name: Name of the asset
            inputs: Input data to hash
            fn: Optional function to compute hash from

        Returns:
            Cached value if found, None otherwise
        """
        if not self.enabled:
            return None

        key = self.compute_cache_key(asset_name, inputs, fn)
        entry = self.backend.get(key)

        if entry is not None:
            return entry.value

        return None

    def set(
        self,
        asset_name: str,
        inputs: Any,
        value: Any,
        fn: Callable[P, T] | None = None,
        ttl: int | None = None,
    ) -> None:
        """
        Cache a result.

        Args:
            asset_name: Name of the asset
            inputs: Input data to hash
            value: Value to cache
            fn: Optional function to compute hash from
            ttl: Time-to-live in seconds
        """
        if not self.enabled:
            return

        key = self.compute_cache_key(asset_name, inputs, fn)
        self.backend.set(key, value, ttl)

    def invalidate(self, asset_name: str, inputs: Any | None = None) -> int:
        """
        Invalidate cache entries for an asset.

        Args:
            asset_name: Name of the asset to invalidate
            inputs: Optional specific inputs to invalidate. If None, invalidates all.

        Returns:
            Number of entries invalidated
        """
        if not self.enabled:
            return 0

        # If specific inputs provided, invalidate just that entry
        if inputs is not None:
            key = self.compute_cache_key(asset_name, inputs)
            return 1 if self.backend.delete(key) else 0

        # Otherwise, we need to clear all entries for this asset
        # This is backend-specific, so we delegate to backend
        # For now, just clear the entire cache
        # TODO: Implement more selective invalidation
        logger.warning(
            f"Invalidating all cache entries for {asset_name} "
            "(full cache clear - selective invalidation not yet implemented)"
        )
        self.backend.clear()
        return 0

    def invalidate_by_asset_name(self, asset_name: str) -> int:
        """
        Invalidate all cache entries for an asset.

        Args:
            asset_name: Name of the asset to invalidate

        Returns:
            Number of entries invalidated
        """
        if not self.enabled:
            return 0

        # For memory cache, we need to iterate and delete
        if isinstance(self.backend, MemoryCacheBackend):
            count = 0
            with self.backend._lock:
                keys_to_delete = [
                    k for k in self.backend._cache.keys() if k.startswith(f"{asset_name}:")
                ]
                for key in keys_to_delete:
                    del self.backend._cache[key]
                    count += 1

            logger.info(f"Invalidated {count} cache entries for {asset_name}")
            return count

        # For disk cache, we need to iterate files
        if isinstance(self.backend, DiskCacheBackend):
            count = 0
            for cache_path in self.backend.cache_dir.glob("*.cache"):
                try:
                    with open(cache_path, "rb") as f:
                        data = pickle.load(f)
                    asset = data["key"]["asset_name"]
                    if asset == asset_name:
                        cache_path.unlink(missing_ok=True)
                        count += 1
                except Exception:
                    pass

            logger.info(f"Invalidated {count} cache entries for {asset_name}")
            return count

        return 0

    def cleanup_expired(self) -> int:
        """
        Clean up expired cache entries.

        Returns:
            Number of entries removed
        """
        return self.backend.cleanup_expired()

    def get_stats(self) -> dict[str, Any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics
        """
        stats = self.backend.get_stats()
        stats["enabled"] = self.enabled
        return stats

    def clear(self) -> None:
        """Clear all cache entries."""
        self.backend.clear()

    @staticmethod
    def _hash_value(value: Any) -> str:
        """
        Hash a value for cache key generation.

        Args:
            value: Value to hash

        Returns:
            Hexadecimal hash string
        """
        try:
            # Try JSON serialization first (deterministic)
            json_str = json.dumps(value, sort_keys=True, default=str)
            return hashlib.sha256(json_str.encode()).hexdigest()
        except (TypeError, ValueError):
            # Fallback to pickle
            try:
                return hashlib.sha256(pickle.dumps(value)).hexdigest()
            except Exception:
                # Final fallback: string hash
                return hashlib.sha256(str(value).encode()).hexdigest()

    @staticmethod
    def _hash_function(fn: Callable[P, T]) -> str:
        """
        Hash a function for cache key generation.

        Args:
            fn: Function to hash

        Returns:
            Hexadecimal hash string
        """
        try:
            # Get source code
            source = inspect.getsource(fn)
            return hashlib.sha256(source.encode()).hexdigest()
        except (TypeError, OSError):
            # Fallback: use function name and module
            return hashlib.sha256(f"{fn.__module__}.{fn.__name__}".encode()).hexdigest()


# =============================================================================
# Decorator
# =============================================================================


def cached(ttl: int | None = None, cache_manager: CacheManager | None = None):
    """
    Decorator to cache function results.

    Args:
        ttl: Time-to-live in seconds (None = no expiration)
        cache_manager: CacheManager instance (creates new one if None)

    Returns:
        Decorator function

    Example:
        Cache a function with 1 hour TTL::

            @cached(ttl=3600)
            def expensive_computation(data):
                return complex_transform(data)
    """

    def decorator(fn: Callable[P, T]) -> Callable[P, T]:
        cache_mgr = cache_manager or CacheManager()

        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # Compute inputs for cache key
            if args and kwargs:
                inputs = (*args, frozenset(kwargs.items()))
            elif args:
                inputs = args
            elif kwargs:
                inputs = frozenset(kwargs.items())
            else:
                inputs = None

            # Try to get from cache
            cached_result = cache_mgr.get(fn.__name__, inputs, fn)

            if cached_result is not None:
                return cached_result

            # Not cached, compute result
            result = fn(*args, **kwargs)

            # Store in cache
            cache_mgr.set(fn.__name__, inputs, result, fn, ttl)

            return result

        wrapper.cache_manager = cache_mgr  # type: ignore
        wrapper.cache_stats = cache_mgr.get_stats  # type: ignore

        return wrapper

    return decorator
