from __future__ import annotations

import hashlib
import math
import re
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# Shared utilities — use these in both ResponseCache and SharedRedisCache
# ---------------------------------------------------------------------------

PRIVACY_PATTERNS = re.compile(
    r"\b(balance|password|credit.card|ssn|social.security|user.\d+|account.\d+)\b",
    re.IGNORECASE,
)


def _is_uncacheable(query: str) -> bool:
    """Return True if query contains privacy-sensitive keywords."""
    return bool(PRIVACY_PATTERNS.search(query))


def _looks_like_false_hit(query: str, cached_key: str) -> bool:
    """Return True if query and cached key contain different 4-digit numbers (years, IDs)."""
    nums_q = set(re.findall(r"\b\d{4}\b", query))
    nums_c = set(re.findall(r"\b\d{4}\b", cached_key))
    return bool(nums_q and nums_c and nums_q != nums_c)


def _redis_error_types() -> tuple[type[Exception], ...]:
    """Errors that mean 'Redis is unreachable', not 'your data is wrong'."""
    try:
        from redis.exceptions import ConnectionError as RedisConnectionError
        from redis.exceptions import RedisError
        from redis.exceptions import TimeoutError as RedisTimeoutError

        return (RedisConnectionError, RedisTimeoutError, RedisError, OSError)
    except Exception:  # noqa: BLE001 - any import failure means we cannot classify redis errors
        return (OSError,)


REDIS_ERRORS: tuple[type[Exception], ...] = _redis_error_types()


# ---------------------------------------------------------------------------
# In-memory cache (existing)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class CacheEntry:
    key: str
    value: str
    created_at: float
    metadata: dict[str, str]


class ResponseCache:
    """In-process semantic cache with privacy and false-hit guardrails.

    Lookup is cosine similarity over word tokens plus character 3-grams, gated by
    `similarity_threshold`.  Two guardrails sit on top of the score:

    - `_is_uncacheable()` bypasses the cache entirely for privacy-sensitive queries, on both
      read and write, so sensitive answers are never stored in the first place.
    - `_looks_like_false_hit()` rejects a match whose 4-digit numbers differ from the query's
      (years, IDs), which is where a purely lexical score is most likely to be confidently
      wrong.  Rejections are appended to `false_hit_log` rather than silently dropped.

    Entries expire lazily on read against `ttl_seconds`.  For multi-instance deployments use
    `SharedRedisCache`, which keeps the same interface and reuses `similarity()`.
    """

    def __init__(self, ttl_seconds: int, similarity_threshold: float):
        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self._entries: list[CacheEntry] = []
        self.false_hit_log: list[dict[str, object]] = []

    def get(self, query: str) -> tuple[str | None, float]:
        """Look up a cached response by semantic similarity, with guardrails."""
        if _is_uncacheable(query):
            return None, 0.0

        now = time.time()
        self._entries = [e for e in self._entries if now - e.created_at <= self.ttl_seconds]

        best_score = 0.0
        best_entry: CacheEntry | None = None
        for entry in self._entries:
            score = self.similarity(query, entry.key)
            if score > best_score:
                best_score = score
                best_entry = entry

        if best_entry is not None and best_score >= self.similarity_threshold:
            if _looks_like_false_hit(query, best_entry.key):
                self.false_hit_log.append(
                    {
                        "query": query,
                        "cached_key": best_entry.key,
                        "score": best_score,
                        "reason": "date_or_number_mismatch",
                    }
                )
                return None, best_score
            return best_entry.value, best_score

        return None, best_score

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        """Store a response in cache, skipping privacy-sensitive queries."""
        if _is_uncacheable(query):
            return
        self._entries.append(
            CacheEntry(key=query, value=value, created_at=time.time(), metadata=metadata or {})
        )

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        """Words plus character 3-grams — keeps word identity and sub-word overlap."""
        normalized = text.lower().strip()
        tokens: list[str] = normalized.split()
        for word in normalized.split():
            if len(word) <= 3:
                tokens.append(word)
                continue
            for i in range(len(word) - 2):
                tokens.append(word[i : i + 3])
        return tokens

    @staticmethod
    def similarity(a: str, b: str) -> float:
        """Cosine similarity over character n-grams + word tokens."""
        if a == b:
            return 1.0
        vec_a = Counter(ResponseCache._tokenize(a))
        vec_b = Counter(ResponseCache._tokenize(b))
        if not vec_a or not vec_b:
            return 0.0
        common = set(vec_a) & set(vec_b)
        dot = sum(vec_a[t] * vec_b[t] for t in common)
        if dot == 0:
            return 0.0
        norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
        norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
        return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# Redis shared cache (new)
# ---------------------------------------------------------------------------


class SharedRedisCache:
    """Redis-backed shared cache for multi-instance deployments.

    Same contract as `ResponseCache` — `get()` returns `(text | None, score)` and `set()`
    applies the same privacy guard — but state lives in Redis, so every replica sees the same
    entries and the cache survives a restart or a rolling deploy.

    If Redis is unreachable, every operation degrades to an in-process `ResponseCache`
    (`local_fallback=True`, the default) and increments `degraded_operations`; a Redis outage
    therefore costs hit rate, not availability.

    Data model:
        Key    = "{prefix}{query_hash}"   (Redis String namespace)
        Value  = Redis Hash with fields:  "query", "response"
        TTL    = Redis EXPIRE (automatic cleanup — no manual eviction)

    For similarity lookup: SCAN all keys with self.prefix, HGET each entry's
    "query" field, compute similarity locally via ResponseCache.similarity().

    Lookup tries the exact hash first (O(1), score 1.0), then falls back to a `SCAN` over the
    prefix, scoring each cached query with `ResponseCache.similarity()` so both backends rank
    matches identically.
    """

    def __init__(
        self,
        redis_url: str,
        ttl_seconds: int,
        similarity_threshold: float,
        prefix: str = "rl:cache:",
        local_fallback: bool = True,
    ):
        import redis as redis_lib

        self.ttl_seconds = ttl_seconds
        self.similarity_threshold = similarity_threshold
        self.prefix = prefix
        self.false_hit_log: list[dict[str, object]] = []
        self._redis: Any = redis_lib.Redis.from_url(redis_url, decode_responses=True)
        # Graceful degradation: if Redis is unreachable the cache keeps working in-process
        # instead of taking the whole gateway down with it.
        self._fallback: ResponseCache | None = (
            ResponseCache(ttl_seconds, similarity_threshold) if local_fallback else None
        )
        self.degraded_operations: int = 0
        self.degradation_log: list[dict[str, object]] = []

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return bool(self._redis.ping())
        except Exception:  # noqa: BLE001 - any failure here means "Redis is not usable"
            return False

    def get(self, query: str) -> tuple[str | None, float]:
        """Look up a cached response from Redis, degrading to the local cache on failure."""
        if _is_uncacheable(query):
            return None, 0.0
        try:
            return self._redis_get(query)
        except REDIS_ERRORS as exc:
            return self._degrade("get", query, exc)

    def _degrade(self, operation: str, query: str, exc: Exception) -> tuple[str | None, float]:
        """Record a Redis outage and serve from the local fallback cache if we have one."""
        self.degraded_operations += 1
        self.degradation_log.append(
            {"reason": "redis_unavailable", "operation": operation, "error": str(exc)}
        )
        if self._fallback is None:
            return None, 0.0
        return self._fallback.get(query)

    def _redis_get(self, query: str) -> tuple[str | None, float]:
        exact_key = f"{self.prefix}{self._query_hash(query)}"
        exact = self._redis.hget(exact_key, "response")
        if exact is not None:
            return str(exact), 1.0

        best_score = 0.0
        best_query: str | None = None
        best_value: str | None = None
        for key in self._redis.scan_iter(f"{self.prefix}*"):
            cached_query = self._redis.hget(key, "query")
            if cached_query is None:
                continue
            score = ResponseCache.similarity(query, str(cached_query))
            if score > best_score:
                best_score = score
                best_query = str(cached_query)
                best_value = self._redis.hget(key, "response")

        if best_value is not None and best_score >= self.similarity_threshold:
            assert best_query is not None
            if _looks_like_false_hit(query, best_query):
                self.false_hit_log.append(
                    {
                        "query": query,
                        "cached_key": best_query,
                        "score": best_score,
                        "reason": "date_or_number_mismatch",
                    }
                )
                return None, best_score
            return str(best_value), best_score

        return None, best_score

    def set(self, query: str, value: str, metadata: dict[str, str] | None = None) -> None:
        """Store a response in Redis with TTL, mirroring into the local fallback cache."""
        if _is_uncacheable(query):
            return
        if self._fallback is not None:
            self._fallback.set(query, value, metadata)
        key = f"{self.prefix}{self._query_hash(query)}"
        try:
            self._redis.hset(key, mapping={"query": query, "response": value})
            self._redis.expire(key, self.ttl_seconds)
        except REDIS_ERRORS as exc:
            self._degrade("set", query, exc)

    def flush(self) -> None:
        """Remove all entries with this cache prefix (for testing)."""
        for key in self._redis.scan_iter(f"{self.prefix}*"):
            self._redis.delete(key)

    def close(self) -> None:
        """Close Redis connection."""
        if self._redis is not None:
            self._redis.close()

    @staticmethod
    def _query_hash(query: str) -> str:
        """Deterministic short hash for a query string."""
        return hashlib.md5(query.lower().strip().encode()).hexdigest()[:12]
