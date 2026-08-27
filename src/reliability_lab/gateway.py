from __future__ import annotations

import threading
from dataclasses import dataclass, field

from reliability_lab.cache import ResponseCache, SharedRedisCache
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitOpenError
from reliability_lab.providers import FakeLLMProvider, ProviderError, ProviderResponse

STATIC_FALLBACK_TEXT = "The service is temporarily degraded. Please try again soon."
BUDGET_EXHAUSTED_TEXT = (
    "The service is running in low-cost mode. Only cached answers are available right now."
)


@dataclass(slots=True)
class GatewayResponse:
    text: str
    route: str
    provider: str | None
    cache_hit: bool
    latency_ms: float
    estimated_cost: float
    error: str | None = None


@dataclass
class BudgetTracker:
    """Cost budget for a run, with a soft degrade point before the hard stop.

    Below `degrade_ratio` of the limit the gateway routes normally.  Above it, providers are
    re-ordered cheapest-first so the expensive primary is skipped while the budget drains.
    At or above the limit no provider is called at all — only the cache can answer.
    """

    limit: float | None = None
    degrade_ratio: float = 0.8
    spent: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record(self, cost: float) -> None:
        with self._lock:
            self.spent += cost

    @property
    def fraction_used(self) -> float:
        if not self.limit:
            return 0.0
        return self.spent / self.limit

    @property
    def should_degrade(self) -> bool:
        """True once spend crosses the soft threshold — prefer cheap providers."""
        return self.limit is not None and self.fraction_used >= self.degrade_ratio

    @property
    def is_exhausted(self) -> bool:
        """True once spend reaches the hard limit — cache or static only."""
        return self.limit is not None and self.fraction_used >= 1.0


class ReliabilityGateway:
    """Routes requests through cache, circuit breakers, and fallback providers."""

    def __init__(
        self,
        providers: list[FakeLLMProvider],
        breakers: dict[str, CircuitBreaker],
        cache: ResponseCache | SharedRedisCache | None = None,
        budget: BudgetTracker | None = None,
        single_flight: bool = True,
        single_flight_timeout_s: float = 30.0,
    ):
        self.providers = providers
        self.breakers = breakers
        self.cache = cache
        self.budget = budget or BudgetTracker()

        # Single-flight: collapse concurrent misses on the same prompt into one provider
        # call.  Without it, N workers that miss simultaneously all call the provider, all
        # write the same entry, and each extra call is another exposure to the provider's
        # failure rate — measurably lowering both hit rate and availability under load.
        self.single_flight = single_flight
        self.single_flight_timeout_s = single_flight_timeout_s
        self._inflight: dict[str, threading.Event] = {}
        self._inflight_lock = threading.Lock()
        self.coalesced_waits = 0  # followers that waited instead of calling a provider
        self.coalesced_hits = 0  # of those, how many got the leader's cached answer

    def _provider_order(self) -> list[tuple[FakeLLMProvider, bool]]:
        """Return (provider, is_primary) pairs in the order they should be attempted.

        Normal mode keeps the configured order.  Budget-degraded mode sorts cheapest-first,
        which demotes the expensive primary without removing it as a last resort.
        """
        primary = self.providers[0] if self.providers else None
        if self.budget.should_degrade:
            ordered = sorted(self.providers, key=lambda p: p.cost_per_1k_tokens)
        else:
            ordered = list(self.providers)
        return [(p, p is primary) for p in ordered]

    def complete(self, prompt: str) -> GatewayResponse:
        """Return a reliable response or a degraded fallback.

        Pipeline: cache -> provider chain guarded by circuit breakers -> static fallback,
        with a cost budget that can demote or cut off provider calls entirely.
        """
        if self.cache is not None:
            cached_text, score = self.cache.get(prompt)
            if cached_text is not None:
                return GatewayResponse(
                    text=cached_text,
                    route=f"cache_hit:{score:.2f}",
                    provider=None,
                    cache_hit=True,
                    latency_ms=0.0,
                    estimated_cost=0.0,
                )

        # Hard budget stop: the cache above was the only affordable path.
        if self.budget.is_exhausted:
            return GatewayResponse(
                text=BUDGET_EXHAUSTED_TEXT,
                route="budget_exhausted",
                provider=None,
                cache_hit=False,
                latency_ms=0.0,
                estimated_cost=0.0,
                error=f"cost budget exhausted ({self.budget.spent:.4f}/{self.budget.limit})",
            )

        if not self.single_flight or self.cache is None:
            return self._call_providers(prompt)
        return self._call_providers_single_flight(prompt)

    def _call_providers_single_flight(self, prompt: str) -> GatewayResponse:
        """Let one caller per prompt reach the providers; the rest wait for its cache write.

        A follower that wakes to a still-empty cache (the leader failed, or timed out) falls
        through to calling the providers itself, so collapsing requests never turns one
        provider failure into many dropped requests.
        """
        with self._inflight_lock:
            existing = self._inflight.get(prompt)
            if existing is None:
                event = threading.Event()
                self._inflight[prompt] = event
                is_leader = True
            else:
                event = existing
                is_leader = False
                self.coalesced_waits += 1

        if is_leader:
            try:
                return self._call_providers(prompt)
            finally:
                with self._inflight_lock:
                    self._inflight.pop(prompt, None)
                event.set()

        event.wait(timeout=self.single_flight_timeout_s)
        if self.cache is not None:
            cached_text, score = self.cache.get(prompt)
            if cached_text is not None:
                with self._inflight_lock:
                    self.coalesced_hits += 1
                return GatewayResponse(
                    text=cached_text,
                    route=f"cache_hit:{score:.2f}",
                    provider=None,
                    cache_hit=True,
                    latency_ms=0.0,
                    estimated_cost=0.0,
                )
        # The leader did not produce a cacheable answer — do the work ourselves.
        return self._call_providers(prompt)

    def _call_providers(self, prompt: str) -> GatewayResponse:
        """Walk the provider chain in order, then fall back to the static message."""
        degraded = self.budget.should_degrade
        last_error: str | None = None
        for provider, is_primary in self._provider_order():
            breaker = self.breakers[provider.name]
            try:
                response: ProviderResponse = breaker.call(provider.complete, prompt)
            except (ProviderError, CircuitOpenError) as exc:
                last_error = f"{provider.name}: {exc}"
                continue

            self.budget.record(response.estimated_cost)
            if self.cache is not None:
                self.cache.set(prompt, response.text, {"provider": provider.name})

            if degraded:
                route = "cost_degraded"
            elif is_primary:
                route = "primary"
            else:
                route = "fallback"
            return GatewayResponse(
                text=response.text,
                route=route,
                provider=provider.name,
                cache_hit=False,
                latency_ms=response.latency_ms,
                estimated_cost=response.estimated_cost,
            )

        return GatewayResponse(
            text=STATIC_FALLBACK_TEXT,
            route="static_fallback",
            provider=None,
            cache_hit=False,
            latency_ms=0.0,
            estimated_cost=0.0,
            error=last_error or "all providers failed",
        )
