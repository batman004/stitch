"""Exceptions the engine understands.

Raise these from your own code (or translate your client library's errors into
them) and the engine knows what to do:

    TransientError      retry this step with backoff
    PermanentError      fail the step; retrying would repeat the mistake
    ProviderError       this provider can't serve us right now: try another one
      QuotaExhausted      out of credits/tokens: long cooldown
      RateLimited         slow down: short cooldown, honours retry_after
      ProviderDown        outage, timeouts, 5xx: breaker opens after a few
    NoProviderAvailable every provider is cooling down (raised by Pool)
    Park                put this item aside until a time, keep going with the rest
"""

from __future__ import annotations


class TransientError(Exception):
    """Worth retrying: timeouts, 5xx, rate limits."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    """Retrying will not help: bad input, 4xx."""


class ProviderError(Exception):
    """One provider can't serve the call. A Pool fails over to the next one."""

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class QuotaExhausted(ProviderError):
    """Out of credits, tokens or budget. Waiting a few seconds won't help."""


class RateLimited(ProviderError):
    """Too many requests. Back off for `retry_after`."""


class ProviderDown(ProviderError):
    """Outage, connection failure, timeout, malformed answer."""


class NoProviderAvailable(Exception):
    """Every provider in the pool is cooling down or failed."""

    def __init__(self, message: str, retry_at: float | None = None):
        super().__init__(message)
        self.retry_at = retry_at  # epoch seconds when the first breaker allows a probe


class Park(Exception):
    """Set this item aside until `until` (epoch seconds) and carry on with others.

    The item's checkpoints are kept. When the time comes the engine runs it
    again and it continues from the step that parked it.
    """

    def __init__(self, until: float, reason: str = ""):
        super().__init__(reason or "parked")
        self.until = until
        self.reason = reason


class StepFailed(Exception):
    def __init__(self, step: str, cause: BaseException, transient: bool):
        super().__init__(f"{step}: {describe(cause)}")
        self.step = step
        self.cause = cause
        self.transient = transient


class WorkflowFailed(Exception):
    """A decorated workflow call ended in failure."""


class WorkflowParked(Exception):
    """A decorated workflow call is parked and the caller chose not to wait."""

    def __init__(self, until: float, reason: str):
        super().__init__(reason)
        self.until = until


def describe(exc: BaseException) -> str:
    if isinstance(exc, TimeoutError):
        return "timed out"
    return str(exc) or type(exc).__name__
