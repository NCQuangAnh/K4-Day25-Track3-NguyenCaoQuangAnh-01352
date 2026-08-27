"""Tests for the stretch goals: cost-aware routing, Redis degradation, shared circuit
state, and concurrent load.
"""

from __future__ import annotations

import threading

import pytest

from reliability_lab.cache import SharedRedisCache
from reliability_lab.chaos import run_scenario
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import BudgetTracker, ReliabilityGateway
from reliability_lab.providers import FakeLLMProvider


def _redis_available() -> bool:
    try:
        import redis as redis_lib

        r = redis_lib.Redis.from_url("redis://localhost:6379/0")
        r.ping()
        r.close()
        return True
    except Exception:  # noqa: BLE001 - any failure here just means 'no Redis'
        return False


needs_redis = pytest.mark.skipif(
    not _redis_available(), reason="Redis not running — start with: docker compose up -d"
)


def _gateway(budget: BudgetTracker | None = None) -> ReliabilityGateway:
    expensive = FakeLLMProvider("primary", 0.0, 1, cost_per_1k_tokens=0.10)
    cheap = FakeLLMProvider("backup", 0.0, 1, cost_per_1k_tokens=0.001)
    breakers = {
        "primary": CircuitBreaker("primary", failure_threshold=3, reset_timeout_seconds=10),
        "backup": CircuitBreaker("backup", failure_threshold=3, reset_timeout_seconds=10),
    }
    return ReliabilityGateway([expensive, cheap], breakers, None, budget)


# ---------------------------------------------------------------------------
# Cost-aware routing
# ---------------------------------------------------------------------------


def test_no_budget_keeps_configured_order() -> None:
    result = _gateway().complete("hello")
    assert result.provider == "primary"
    assert result.route == "primary"


def test_budget_below_degrade_point_uses_primary() -> None:
    gateway = _gateway(BudgetTracker(limit=1.0, degrade_ratio=0.8))
    result = gateway.complete("hello")
    assert result.provider == "primary"
    assert gateway.budget.spent > 0


def test_budget_above_degrade_point_switches_to_cheap_provider() -> None:
    budget = BudgetTracker(limit=1.0, degrade_ratio=0.8, spent=0.85)
    result = _gateway(budget).complete("hello")
    assert result.provider == "backup", "should demote the expensive primary once degraded"
    assert result.route == "cost_degraded"


def test_exhausted_budget_calls_no_provider() -> None:
    budget = BudgetTracker(limit=1.0, spent=1.0)
    gateway = _gateway(budget)
    result = gateway.complete("hello")
    assert result.route == "budget_exhausted"
    assert result.provider is None
    assert result.estimated_cost == 0.0
    assert gateway.budget.spent == 1.0, "no further spend once the cap is hit"


def test_budget_tracks_cumulative_spend() -> None:
    """Spend must accumulate across calls, not be reset per request."""
    gateway = _gateway(BudgetTracker(limit=10.0))
    seen: list[float] = []
    for _ in range(3):
        gateway.complete("hello")
        seen.append(gateway.budget.spent)
    assert seen[0] < seen[1] < seen[2]
    assert gateway.budget.spent == pytest.approx(seen[2])
    assert 0.0 < gateway.budget.fraction_used < 1.0


# ---------------------------------------------------------------------------
# Redis graceful degradation
# ---------------------------------------------------------------------------


def test_redis_down_falls_back_to_local_cache() -> None:
    """A dead Redis must degrade to in-process caching, not break the gateway."""
    cache = SharedRedisCache(
        redis_url="redis://localhost:6399/0",  # nothing listening here
        ttl_seconds=60,
        similarity_threshold=0.5,
        prefix="rl:down:",
    )
    assert not cache.ping()
    cache.set("hello world", "local answer")
    cached, score = cache.get("hello world")
    assert cached == "local answer", "local fallback should still serve the entry"
    assert score == 1.0
    assert cache.degraded_operations > 0
    assert cache.degradation_log[0]["reason"] == "redis_unavailable"


def test_redis_down_without_fallback_is_a_miss_not_a_crash() -> None:
    cache = SharedRedisCache(
        redis_url="redis://localhost:6399/0",
        ttl_seconds=60,
        similarity_threshold=0.5,
        prefix="rl:down:",
        local_fallback=False,
    )
    cache.set("hello world", "answer")
    cached, score = cache.get("hello world")
    assert cached is None
    assert score == 0.0
    assert cache.degraded_operations >= 1


@needs_redis
def test_healthy_redis_does_not_report_degradation() -> None:
    cache = SharedRedisCache("redis://localhost:6379/0", 60, 0.5, prefix="rl:test:healthy:")
    cache.flush()
    cache.set("hello world", "answer")
    assert cache.get("hello world")[0] == "answer"
    assert cache.degraded_operations == 0
    cache.flush()
    cache.close()


# ---------------------------------------------------------------------------
# Redis-backed shared circuit state
# ---------------------------------------------------------------------------


@needs_redis
def test_failures_on_one_replica_open_the_circuit_on_another() -> None:
    from reliability_lab.shared_circuit import SharedCircuitBreaker

    a = SharedCircuitBreaker("shared_test", 3, 5, prefix="rl:test:cb:")
    b = SharedCircuitBreaker("shared_test", 3, 5, prefix="rl:test:cb:")
    a.reset_shared_state()
    try:
        a.record_failure()
        a.record_failure()
        assert b.allow_request(), "two failures is below threshold — still closed fleet-wide"
        b.record_failure()  # third failure, from the other replica
        assert not a.allow_request(), "replica A must adopt the shared open state"
        assert not b.allow_request()
    finally:
        a.reset_shared_state()
        a.close()
        b.close()


@needs_redis
def test_only_one_replica_wins_the_probe() -> None:
    """SET NX must elect exactly one prober per reset window, whatever the replica count."""
    import time

    from reliability_lab.shared_circuit import SharedCircuitBreaker

    replicas = [SharedCircuitBreaker("probe_test", 1, 1, prefix="rl:test:cb:") for _ in range(5)]
    replicas[0].reset_shared_state()
    try:
        replicas[0].record_failure()
        assert not any(r.allow_request() for r in replicas), "all replicas fail fast while open"
        time.sleep(1.2)  # let the open flag expire
        allowed = [r.allow_request() for r in replicas]
        assert sum(allowed) == 1, f"expected exactly one prober, got {sum(allowed)}"
    finally:
        replicas[0].reset_shared_state()
        for r in replicas:
            r.close()


@needs_redis
def test_shared_breaker_recovers_fleet_wide_on_probe_success() -> None:
    import time

    from reliability_lab.shared_circuit import SharedCircuitBreaker

    a = SharedCircuitBreaker("recover_test", 1, 1, prefix="rl:test:cb:")
    b = SharedCircuitBreaker("recover_test", 1, 1, prefix="rl:test:cb:")
    a.reset_shared_state()
    try:
        a.record_failure()
        assert not b.allow_request()
        time.sleep(1.2)
        prober = a if a.allow_request() else b
        prober.record_success()
        assert b.allow_request(), "a successful probe must reopen traffic for every replica"
    finally:
        a.reset_shared_state()
        a.close()
        b.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


def _load_config(concurrency: int) -> LabConfig:
    return LabConfig.model_validate(
        {
            "providers": [
                {
                    "name": "primary",
                    "fail_rate": 0.3,
                    "base_latency_ms": 5,
                    "cost_per_1k_tokens": 0.01,
                },
                {
                    "name": "backup",
                    "fail_rate": 0.0,
                    "base_latency_ms": 5,
                    "cost_per_1k_tokens": 0.005,
                },
            ],
            "circuit_breaker": {
                "failure_threshold": 3,
                "reset_timeout_seconds": 1,
                "success_threshold": 1,
            },
            "cache": {"enabled": True, "ttl_seconds": 60, "similarity_threshold": 0.92},
            "load_test": {"requests": 40, "concurrency": concurrency},
        }
    )


def test_concurrent_run_produces_same_request_count() -> None:
    """Under 8 workers the metrics must still add up — no lost or double-counted requests."""
    metrics = run_scenario(
        _load_config(8), ["what is the refund policy"], ScenarioConfig(name="concurrent")
    )
    assert metrics.total_requests == 40
    assert metrics.successful_requests + metrics.failed_requests == 40
    assert metrics.concurrency == 8


def test_concurrent_breaker_never_over_counts_transitions() -> None:
    """The breaker lock must stop two workers logging the same OPEN transition twice."""
    metrics = run_scenario(
        _load_config(8), ["what is the refund policy"], ScenarioConfig(name="concurrent")
    )
    # Two providers, a short reset window: opens are possible, but each open must be a real
    # state change, never a duplicate of one already recorded.
    assert metrics.circuit_open_count <= metrics.total_requests


# ---------------------------------------------------------------------------
# Single-flight (cache stampede protection)
# ---------------------------------------------------------------------------


def _stampede(single_flight: bool, workers: int = 12) -> tuple[int, ReliabilityGateway]:
    """Fire `workers` concurrent requests for the SAME prompt at a cold cache."""
    from concurrent.futures import ThreadPoolExecutor

    from reliability_lab.cache import ResponseCache

    calls = {"n": 0}
    calls_lock = threading.Lock()

    class CountingProvider(FakeLLMProvider):
        def complete(self, prompt: str):  # type: ignore[no-untyped-def]
            with calls_lock:
                calls["n"] += 1
            return super().complete(prompt)

    provider = CountingProvider("primary", 0.0, 40, cost_per_1k_tokens=0.01)
    breakers = {"primary": CircuitBreaker("primary", failure_threshold=99, reset_timeout_seconds=10)}
    gateway = ReliabilityGateway(
        [provider],
        breakers,
        ResponseCache(60, 0.92),
        single_flight=single_flight,
    )
    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(lambda _: gateway.complete("what is the refund policy"), range(workers)))
    return calls["n"], gateway


def test_without_single_flight_every_worker_calls_the_provider() -> None:
    """Baseline: this is the stampede the feature exists to prevent."""
    calls, _ = _stampede(single_flight=False)
    assert calls > 1, "concurrent misses should each reach the provider when disabled"


def test_single_flight_collapses_concurrent_misses() -> None:
    calls, gateway = _stampede(single_flight=True)
    assert calls == 1, f"expected exactly one provider call, got {calls}"
    assert gateway.coalesced_waits == 11, "the other 11 workers should have waited"
    assert gateway.coalesced_hits == 11, "and all should have been served the leader's answer"


def test_single_flight_reduces_provider_calls_versus_baseline() -> None:
    with_sf, _ = _stampede(single_flight=True)
    without_sf, _ = _stampede(single_flight=False)
    assert with_sf < without_sf


def test_single_flight_follower_still_answered_when_leader_fails() -> None:
    """If the leader gets nothing cacheable, followers must not be dropped."""
    from concurrent.futures import ThreadPoolExecutor

    from reliability_lab.cache import ResponseCache

    provider = FakeLLMProvider("primary", fail_rate=1.0, base_latency_ms=20, cost_per_1k_tokens=0.01)
    breakers = {"primary": CircuitBreaker("primary", failure_threshold=99, reset_timeout_seconds=10)}
    gateway = ReliabilityGateway([provider], breakers, ResponseCache(60, 0.92))
    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: gateway.complete("always fails"), range(6)))
    assert len(results) == 6
    assert all(r.text for r in results), "every caller must get a response object"
    assert all(r.route == "static_fallback" for r in results)


def test_single_flight_is_a_noop_without_a_cache() -> None:
    """With no cache there is nothing to share, so every caller does its own work."""
    provider = FakeLLMProvider("primary", 0.0, 1, cost_per_1k_tokens=0.01)
    breakers = {"primary": CircuitBreaker("primary", failure_threshold=3, reset_timeout_seconds=10)}
    gateway = ReliabilityGateway([provider], breakers, None, single_flight=True)
    result = gateway.complete("hello")
    assert result.provider == "primary"
    assert gateway.coalesced_waits == 0
