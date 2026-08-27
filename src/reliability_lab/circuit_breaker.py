from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a circuit is open and calls should fail fast."""


@dataclass(slots=True)
class CircuitBreaker:
    """Three-state circuit breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED.

    - CLOSED: calls pass through and consecutive failures are counted.  Reaching
      `failure_threshold` opens the circuit with reason "failure_threshold_reached".
    - OPEN: calls fail fast with `CircuitOpenError` until `reset_timeout_seconds` elapses,
      at which point the next `allow_request()` promotes the circuit to HALF_OPEN.
    - HALF_OPEN: a probe is allowed through.  `success_threshold` successes close the circuit
      ("probe_success"); a single failure re-opens it ("probe_failure") — a distinct reason
      from the threshold case, so the transition log says *why* each outage started.

    There is no retry loop: a caller that gets an error moves to the next provider rather
    than retrying this one, which is what keeps a provider outage from becoming a retry storm.

    `lock` guards the counters and the transition log so the breaker stays correct under the
    concurrent load in `chaos.run_scenario`.  `SharedCircuitBreaker` extends this class to
    share state across replicas via Redis.
    """

    name: str
    failure_threshold: int
    reset_timeout_seconds: float
    success_threshold: int = 1
    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    opened_at: float | None = None
    transition_log: list[dict[str, str | float]] = field(default_factory=list)
    # Guards the counters and the transition log so concurrent workers cannot interleave a
    # read-modify-write and trip (or miss) the threshold.
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)

    def allow_request(self) -> bool:
        """Return whether a request should be attempted."""
        with self.lock:
            if self.state is CircuitState.CLOSED:
                return True
            if self.state is CircuitState.HALF_OPEN:
                return True
            # OPEN: allow a probe once the reset timeout has elapsed.
            opened_at = self.opened_at if self.opened_at is not None else time.monotonic()
            if time.monotonic() - opened_at >= self.reset_timeout_seconds:
                self._transition(CircuitState.HALF_OPEN, "reset_timeout_elapsed")
                self.success_count = 0
                return True
            return False

    def call(self, fn: Callable[..., T], *args: object, **kwargs: object) -> T:
        """Call a function through the circuit breaker."""
        if not self.allow_request():
            raise CircuitOpenError(f"circuit '{self.name}' is open")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def record_success(self) -> None:
        """Record a successful call."""
        with self.lock:
            self.failure_count = 0
            self.success_count += 1
            if (
                self.state is CircuitState.HALF_OPEN
                and self.success_count >= self.success_threshold
            ):
                self._transition(CircuitState.CLOSED, "probe_success")
                self.success_count = 0
                self.opened_at = None

    def record_failure(self) -> None:
        """Record a failed call."""
        with self.lock:
            self.failure_count += 1
            self.success_count = 0
            if self.state is CircuitState.HALF_OPEN:
                self._transition(CircuitState.OPEN, "probe_failure")
                self.opened_at = time.monotonic()
            elif self.failure_count >= self.failure_threshold:
                self._transition(CircuitState.OPEN, "failure_threshold_reached")
                self.opened_at = time.monotonic()

    def _transition(self, new_state: CircuitState, reason: str) -> None:
        if self.state == new_state:
            return
        self.transition_log.append(
            {"from": self.state.value, "to": new_state.value, "reason": reason, "ts": time.time()}
        )
        self.state = new_state
