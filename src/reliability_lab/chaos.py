from __future__ import annotations

import json
import random
import threading
import zlib
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker
from reliability_lab.config import LabConfig, ScenarioConfig
from reliability_lab.gateway import BudgetTracker, GatewayResponse, ReliabilityGateway
from reliability_lab.metrics import RunMetrics
from reliability_lab.providers import FakeLLMProvider
from reliability_lab.shared_circuit import SharedCircuitBreaker


def load_queries(path: str | Path = "data/sample_queries.jsonl") -> list[str]:
    queries: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        queries.append(json.loads(line)["query"])
    return queries


def build_gateway(
    config: LabConfig,
    provider_overrides: dict[str, float] | None = None,
    budget_limit: float | None = None,
    shared_breakers: bool = False,
) -> ReliabilityGateway:
    providers = []
    for p in config.providers:
        fail_rate = provider_overrides.get(p.name, p.fail_rate) if provider_overrides else p.fail_rate
        providers.append(FakeLLMProvider(p.name, fail_rate, p.base_latency_ms, p.cost_per_1k_tokens))
    breakers: dict[str, CircuitBreaker] = {}
    for p in config.providers:
        if shared_breakers:
            shared = SharedCircuitBreaker(
                name=p.name,
                failure_threshold=config.circuit_breaker.failure_threshold,
                reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
                success_threshold=config.circuit_breaker.success_threshold,
                redis_url=config.cache.redis_url,
            )
            shared.reset_shared_state()
            breakers[p.name] = shared
        else:
            breakers[p.name] = CircuitBreaker(
                name=p.name,
                failure_threshold=config.circuit_breaker.failure_threshold,
                reset_timeout_seconds=config.circuit_breaker.reset_timeout_seconds,
                success_threshold=config.circuit_breaker.success_threshold,
            )
    cache: ResponseCache | SharedRedisCache | None = None
    if config.cache.enabled:
        if config.cache.backend == "redis":
            cache = SharedRedisCache(
                config.cache.redis_url,
                config.cache.ttl_seconds,
                config.cache.similarity_threshold,
            )
        else:
            cache = ResponseCache(config.cache.ttl_seconds, config.cache.similarity_threshold)
    limit = budget_limit if budget_limit is not None else config.budget.limit
    budget = BudgetTracker(limit=limit, degrade_ratio=config.budget.degrade_ratio)
    return ReliabilityGateway(providers, breakers, cache, budget)


def calculate_recovery_time_ms(gateway: ReliabilityGateway) -> float | None:
    """Average time (ms) between a circuit opening and closing again."""
    recoveries: list[float] = []
    for breaker in gateway.breakers.values():
        opened_ts: float | None = None
        for entry in breaker.transition_log:
            ts = float(entry["ts"])
            if entry["to"] == "open":
                opened_ts = ts
            elif entry["to"] == "closed" and opened_ts is not None:
                recoveries.append((ts - opened_ts) * 1000.0)
                opened_ts = None
    if not recoveries:
        return None
    return sum(recoveries) / len(recoveries)


def run_scenario(
    config: LabConfig,
    queries: list[str],
    scenario: ScenarioConfig,
    shared_breakers: bool = False,
    seed: int | None = None,
) -> RunMetrics:
    """Run a single named chaos scenario and collect metrics.

    Runs sequentially when concurrency is 1, otherwise fans the load out across a
    ThreadPoolExecutor — which is what a real gateway sees, and what makes the circuit
    breaker's locking matter.

    Passing `seed` re-seeds the RNG from the scenario name, so each scenario is reproducible
    independently of the ones before it.  Note that a scenario with concurrency > 1 is only
    reproducible in aggregate: worker threads draw from the shared RNG in a nondeterministic
    order, so its per-request results vary between runs by a few percent.
    """
    # Re-seed per scenario so one scenario's RNG draw cannot shift the next one's results.
    # crc32 (not hash()) because Python randomises str hashing per process.
    if seed is not None:
        random.seed(seed + zlib.crc32(scenario.name.encode()))

    gateway = build_gateway(
        config,
        scenario.provider_overrides or None,
        budget_limit=scenario.budget_limit,
        shared_breakers=shared_breakers,
    )
    metrics = RunMetrics()
    metrics_lock = threading.Lock()
    concurrency = scenario.concurrency or config.load_test.concurrency

    def record(result: GatewayResponse) -> None:
        with metrics_lock:
            metrics.total_requests += 1
            metrics.estimated_cost += result.estimated_cost

            if result.cache_hit:
                metrics.cache_hits += 1
                metrics.estimated_cost_saved += 0.001
                metrics.successful_requests += 1
            elif result.route in {"fallback", "cost_degraded"}:
                metrics.fallback_successes += 1
                metrics.successful_requests += 1
            elif result.route in {"static_fallback", "budget_exhausted"}:
                metrics.static_fallbacks += 1
                metrics.failed_requests += 1
            else:
                metrics.successful_requests += 1

            if result.latency_ms > 0:
                metrics.latencies_ms.append(result.latency_ms)

    # Draw the prompts up front, on this thread, so the query sequence is identical whatever
    # the worker count.  Without this the pool's threads race for the shared RNG and even the
    # query mix differs between runs.
    prompts = [random.choice(queries) for _ in range(config.load_test.requests)]

    def one_request(index: int) -> None:
        record(gateway.complete(prompts[index]))

    total = config.load_test.requests
    if concurrency <= 1:
        for i in range(total):
            one_request(i)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(one_request, range(total)))

    metrics.circuit_open_count = sum(
        1
        for breaker in gateway.breakers.values()
        for entry in breaker.transition_log
        if entry["to"] == "open"
    )
    metrics.recovery_time_ms = calculate_recovery_time_ms(gateway)
    metrics.concurrency = concurrency
    metrics.coalesced_waits = gateway.coalesced_waits
    metrics.coalesced_hits = gateway.coalesced_hits
    metrics.budget_limit = gateway.budget.limit
    metrics.budget_spent = gateway.budget.spent
    for breaker in gateway.breakers.values():
        if isinstance(breaker, SharedCircuitBreaker):
            breaker.close()
    return metrics


SCENARIO_CRITERIA: dict[str, Callable[[RunMetrics], bool]] = {
    # Primary always fails: traffic must survive via backup/cache, never mostly static.
    "primary_timeout_100": lambda m: m.availability >= 0.95 and m.fallback_success_rate >= 0.9,
    # Half the primary calls fail: breaker should trip at least once and still stay available.
    "primary_flaky_50": lambda m: m.availability >= 0.95 and m.circuit_open_count >= 1,
    # Baseline: high availability; a rare double-provider failure (0.25 * 0.05) is tolerated.
    "all_healthy": lambda m: m.availability >= 0.95 and m.static_fallbacks <= 0.05 * m.total_requests,
    # Cache disabled comparison run: still available, just no cache hits.
    "no_cache_baseline": lambda m: m.availability >= 0.95 and m.cache_hits == 0,
    # Cost cap: the budget must actually hold — spend may not overshoot the limit by more
    # than one in-flight request, and the run must still answer from cache while degraded.
    "cost_budget_squeeze": lambda m: (
        m.budget_limit is not None and m.budget_spent <= m.budget_limit * 1.2 and m.cache_hits > 0
    ),
    # Concurrent load: same availability guarantee as sequential, under 8 workers.
    "concurrent_load_8": lambda m: m.availability >= 0.95 and m.concurrency > 1,
}


def _scenario_passed(scenario: ScenarioConfig, result: RunMetrics) -> bool:
    """Apply the scenario-specific criterion, defaulting to 'something succeeded'."""
    criterion = SCENARIO_CRITERIA.get(scenario.name)
    if criterion is None:
        return result.successful_requests > 0
    return criterion(result)


def run_simulation(config: LabConfig, queries: list[str], seed: int | None = None) -> RunMetrics:
    """Run all named scenarios from config, or a default run if none defined.

    Always prepends a `no_cache_baseline` run — the same healthy config with the cache
    disabled — so the report can attribute cost and hit-rate differences to the cache rather
    than to run-to-run noise.  Its status is recorded in `scenarios`, but its counters are
    kept out of the aggregate so the pooled metrics describe the configured system.
    """
    if not config.scenarios:
        default_scenario = ScenarioConfig(name="default", description="baseline run")
        metrics = run_scenario(config, queries, default_scenario, seed=seed)
        metrics.scenarios = {"default": "pass" if metrics.successful_requests > 0 else "fail"}
        return metrics

    combined = RunMetrics()
    scenario_list = list(config.scenarios)

    # Cache vs no-cache comparison: same healthy config, cache disabled.
    no_cache_config = config.model_copy(deep=True)
    no_cache_config.cache.enabled = False
    no_cache_result = run_scenario(
        no_cache_config,
        queries,
        ScenarioConfig(name="no_cache_baseline", description="cache disabled comparison run"),
        seed=seed,
    )
    combined.scenarios["no_cache_baseline"] = (
        "pass"
        if _scenario_passed(
            ScenarioConfig(name="no_cache_baseline"), no_cache_result
        )
        else "fail"
    )

    for scenario in scenario_list:
        result = run_scenario(config, queries, scenario, seed=seed)

        passed = _scenario_passed(scenario, result)
        combined.scenarios[scenario.name] = "pass" if passed else "fail"

        combined.total_requests += result.total_requests
        combined.successful_requests += result.successful_requests
        combined.failed_requests += result.failed_requests
        combined.fallback_successes += result.fallback_successes
        combined.static_fallbacks += result.static_fallbacks
        combined.cache_hits += result.cache_hits
        combined.circuit_open_count += result.circuit_open_count
        combined.estimated_cost += result.estimated_cost
        combined.estimated_cost_saved += result.estimated_cost_saved
        combined.latencies_ms.extend(result.latencies_ms)
        combined.budget_spent += result.budget_spent
        combined.concurrency = max(combined.concurrency, result.concurrency)
        combined.coalesced_waits += result.coalesced_waits
        combined.coalesced_hits += result.coalesced_hits
        if result.recovery_time_ms is not None:
            if combined.recovery_time_ms is None:
                combined.recovery_time_ms = result.recovery_time_ms
            else:
                combined.recovery_time_ms = (combined.recovery_time_ms + result.recovery_time_ms) / 2

    return combined
