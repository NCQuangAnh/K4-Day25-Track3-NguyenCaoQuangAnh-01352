"""Redis-backed circuit breaker state for multi-instance deployments.

The in-memory `CircuitBreaker` protects one process.  With N replicas behind a load
balancer each replica has to rediscover an outage on its own — N x failure_threshold wasted
calls — and each replica sends its own HALF_OPEN probe every reset window, which is a
fleet-level retry storm against a provider that is already down.

`SharedCircuitBreaker` moves the two pieces of state that must be global into Redis:

    rl:cb:<name>:failures   INCR + EXPIRE   rolling failure count across all replicas
    rl:cb:<name>:open       SET NX EX       the open flag, and the probe election

`SET ... NX` is what makes the probe safe: when the open flag expires, exactly one replica
wins the race to re-create it and is allowed to send the probe.  Every other replica sees
the key already present and keeps failing fast.  Fleet-wide probe traffic is therefore one
request per reset window regardless of replica count.

If Redis is unreachable the breaker degrades to the local in-memory state machine, so a
Redis outage weakens protection instead of disabling it.
"""

from __future__ import annotations

import time
from typing import Any

from reliability_lab.cache import REDIS_ERRORS
from reliability_lab.circuit_breaker import CircuitBreaker, CircuitState


class SharedCircuitBreaker(CircuitBreaker):
    """CircuitBreaker whose failure count and open flag live in Redis."""

    def __init__(
        self,
        name: str,
        failure_threshold: int,
        reset_timeout_seconds: float,
        success_threshold: int = 1,
        redis_url: str = "redis://localhost:6379/0",
        prefix: str = "rl:cb:",
        failure_window_seconds: int = 60,
    ):
        super().__init__(
            name=name,
            failure_threshold=failure_threshold,
            reset_timeout_seconds=reset_timeout_seconds,
            success_threshold=success_threshold,
        )
        import redis as redis_lib

        self.prefix = prefix
        self.failure_window_seconds = failure_window_seconds
        self.degraded_operations = 0
        self._redis: Any = redis_lib.Redis.from_url(redis_url, decode_responses=True)

    # -- key helpers ---------------------------------------------------------

    @property
    def _failures_key(self) -> str:
        return f"{self.prefix}{self.name}:failures"

    @property
    def _open_key(self) -> str:
        return f"{self.prefix}{self.name}:open"

    def reset_shared_state(self) -> None:
        """Clear this breaker's Redis keys (used by tests and between chaos scenarios)."""
        try:
            self._redis.delete(self._failures_key, self._open_key)
        except REDIS_ERRORS:
            self.degraded_operations += 1
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.opened_at = None
        self.transition_log.clear()

    # -- state machine overrides --------------------------------------------

    def allow_request(self) -> bool:
        """Allow a request unless the shared open flag is set.

        When the flag has expired, `SET NX` elects exactly one replica to probe.
        """
        try:
            open_flag = self._redis.get(self._open_key)
        except REDIS_ERRORS:
            self.degraded_operations += 1
            return super().allow_request()

        if open_flag is None:
            if self.state is CircuitState.CLOSED:
                return True
            # The open flag expired: race for the right to probe.  SET NX means exactly one
            # replica in the fleet wins and re-arms the flag; the losers keep failing fast.
            return self._win_probe_election()

        if self.state is CircuitState.CLOSED:
            # Another replica opened the circuit; adopt the outage locally.
            self._transition(CircuitState.OPEN, "shared_open_flag")
            self.opened_at = time.monotonic()
        return False

    def record_failure(self) -> None:
        """Increment the shared failure count and open the circuit fleet-wide when it trips."""
        try:
            failures = int(self._redis.incr(self._failures_key))
            self._redis.expire(self._failures_key, self.failure_window_seconds)
        except REDIS_ERRORS:
            self.degraded_operations += 1
            super().record_failure()
            return

        self.failure_count = failures
        self.success_count = 0

        if self.state is CircuitState.HALF_OPEN:
            self._open_shared("probe_failure")
        elif failures >= self.failure_threshold:
            self._open_shared("failure_threshold_reached")

    def record_success(self) -> None:
        """Clear the shared failure count and close the circuit fleet-wide on a good probe."""
        was_half_open = self.state is CircuitState.HALF_OPEN
        try:
            self._redis.delete(self._failures_key)
            if was_half_open:
                self._redis.delete(self._open_key)
        except REDIS_ERRORS:
            self.degraded_operations += 1

        self.failure_count = 0
        self.success_count += 1
        if was_half_open and self.success_count >= self.success_threshold:
            self._transition(CircuitState.CLOSED, "probe_success")
            self.success_count = 0
            self.opened_at = None

    # -- internals -----------------------------------------------------------

    def _win_probe_election(self) -> bool:
        """Try to claim the single HALF_OPEN probe slot for this reset window."""
        try:
            won = bool(
                self._redis.set(
                    self._open_key, "probing", nx=True, ex=int(self.reset_timeout_seconds)
                )
            )
        except REDIS_ERRORS:
            self.degraded_operations += 1
            return super().allow_request()

        if not won:
            return False
        self._transition(CircuitState.HALF_OPEN, "probe_election_won")
        self.success_count = 0
        return True

    def _open_shared(self, reason: str) -> None:
        """Set the shared open flag with NX so only one replica owns each probe window."""
        try:
            self._redis.set(self._open_key, reason, nx=True, ex=int(self.reset_timeout_seconds))
        except REDIS_ERRORS:
            self.degraded_operations += 1
        self._transition(CircuitState.OPEN, reason)
        self.opened_at = time.monotonic()

    def close(self) -> None:
        if self._redis is not None:
            self._redis.close()
