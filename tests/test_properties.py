"""Property-based tests (stretch goal).

The example-based tests in `test_circuit_breaker.py` check the transitions we thought of.
These fuzz the state machine with random call sequences and assert the invariants that must
hold for *every* sequence — which is where the interesting bugs actually live.
"""

from __future__ import annotations

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from reliability_lab.cache import ResponseCache
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitState

# ---------------------------------------------------------------------------
# Circuit breaker invariants
# ---------------------------------------------------------------------------


class CircuitBreakerMachine(RuleBasedStateMachine):
    """Drive a breaker with random successes/failures and check it never lies about state."""

    def __init__(self) -> None:
        super().__init__()
        # A long reset timeout keeps the machine deterministic: nothing transitions on a
        # wall-clock timer mid-run, so every state change must come from a recorded call.
        self.cb = CircuitBreaker(
            "fuzz", failure_threshold=3, reset_timeout_seconds=3600, success_threshold=2
        )

    @rule()
    def success(self) -> None:
        self.cb.record_success()

    @rule()
    def failure(self) -> None:
        self.cb.record_failure()

    @rule()
    def probe_gate(self) -> None:
        self.cb.allow_request()

    @invariant()
    def counters_never_negative(self) -> None:
        assert self.cb.failure_count >= 0
        assert self.cb.success_count >= 0

    @invariant()
    def open_implies_opened_at(self) -> None:
        """An OPEN circuit must know when it opened, or the reset timer is meaningless."""
        if self.cb.state is CircuitState.OPEN:
            assert self.cb.opened_at is not None

    @invariant()
    def open_circuit_denies_requests(self) -> None:
        """With a 1 hour reset timeout, an OPEN circuit must always fail fast."""
        if self.cb.state is CircuitState.OPEN:
            assert not self.cb.allow_request()

    @invariant()
    def closed_circuit_is_below_threshold(self) -> None:
        """A CLOSED circuit can never be sitting on a tripped failure count."""
        if self.cb.state is CircuitState.CLOSED:
            assert self.cb.failure_count < self.cb.failure_threshold

    @invariant()
    def transitions_are_never_self_loops(self) -> None:
        """`_transition` must drop no-op changes, otherwise the log over-counts outages."""
        for entry in self.cb.transition_log:
            assert entry["from"] != entry["to"]

    @invariant()
    def open_transitions_have_a_known_reason(self) -> None:
        reasons = {"probe_failure", "failure_threshold_reached"}
        for entry in self.cb.transition_log:
            if entry["to"] == "open":
                assert entry["reason"] in reasons


TestCircuitBreakerMachine = CircuitBreakerMachine.TestCase
TestCircuitBreakerMachine.settings = settings(
    max_examples=200, stateful_step_count=40, deadline=None
)


@given(
    failures=st.integers(min_value=0, max_value=20),
    threshold=st.integers(min_value=1, max_value=5),
)
@settings(max_examples=100, deadline=None)
def test_opens_exactly_at_threshold(failures: int, threshold: int) -> None:
    """N consecutive failures open the circuit if and only if N >= failure_threshold."""
    cb = CircuitBreaker("prop", failure_threshold=threshold, reset_timeout_seconds=3600)
    for _ in range(failures):
        cb.record_failure()
    if failures >= threshold:
        assert cb.state is CircuitState.OPEN
        assert not cb.allow_request()
    else:
        assert cb.state is CircuitState.CLOSED
        assert cb.allow_request()


@given(failures=st.integers(min_value=1, max_value=10))
@settings(max_examples=100, deadline=None)
def test_one_success_always_resets_failure_count(failures: int) -> None:
    """No matter how many failures preceded it, a success zeroes the counter."""
    cb = CircuitBreaker("prop", failure_threshold=100, reset_timeout_seconds=3600)
    for _ in range(failures):
        cb.record_failure()
    cb.record_success()
    assert cb.failure_count == 0


@given(n=st.integers(min_value=1, max_value=10))
@settings(max_examples=50, deadline=None)
def test_repeated_failures_log_one_open_transition(n: int) -> None:
    """A dead provider must not append an OPEN entry per failed call — that inflates metrics."""
    cb = CircuitBreaker("prop", failure_threshold=1, reset_timeout_seconds=3600)
    for _ in range(n):
        cb.record_failure()
    assert len([t for t in cb.transition_log if t["to"] == "open"]) == 1


# ---------------------------------------------------------------------------
# Similarity invariants
# ---------------------------------------------------------------------------

TEXT = st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=40)


@given(a=TEXT, b=TEXT)
@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.filter_too_much])
def test_similarity_is_bounded_and_symmetric(a: str, b: str) -> None:
    """Cosine similarity must stay in [0, 1] and not depend on argument order."""
    forward = ResponseCache.similarity(a, b)
    backward = ResponseCache.similarity(b, a)
    assert 0.0 <= forward <= 1.0
    assert abs(forward - backward) < 1e-9


@given(a=TEXT)
@settings(max_examples=100, deadline=None)
def test_similarity_of_identical_strings_is_one(a: str) -> None:
    assert ResponseCache.similarity(a, a) == 1.0


@given(a=TEXT, b=TEXT)
@settings(max_examples=200, deadline=None)
def test_cache_roundtrip_respects_threshold(a: str, b: str) -> None:
    """A stored entry is returned only when it clears the configured threshold."""
    assume(a != b)
    cache = ResponseCache(ttl_seconds=60, similarity_threshold=0.9)
    cache.set(a, "stored")
    cached, score = cache.get(b)
    if cached is not None:
        assert score >= 0.9
    else:
        assert score < 0.9 or cached is None
