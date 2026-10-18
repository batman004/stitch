class TransientError(Exception):
    """A failure that may succeed if tried again, such as a dropped connection.

    Retried by the default ``RetryPolicy``. ``retry_after`` sets the wait in seconds before the next try, overriding the backoff. A critical step that fails with this error halts the whole run.
    """

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    """A failure that will not go away by retrying, such as invalid input.

    Never retried. The item is marked failed, even in a critical step, and the rest of the batch continues.
    """


class ProviderError(Exception):
    """Base class for failures raised by a ``ProviderPool`` client. The pool records them and fails over to the next provider.

    ``retry_after`` overrides the breaker cooldown in seconds.
    """

    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class QuotaExhausted(ProviderError):
    """The provider is out of credits or quota. Opens its breaker at once for ``BreakerTuning.quota_cooldown_seconds``."""


class RateLimited(ProviderError):
    """The provider asked to slow down (HTTP 429). Opens its breaker at once for ``BreakerTuning.rate_limit_cooldown_seconds``."""


class ProviderDown(ProviderError):
    """The provider is unreachable or erroring. Opens its breaker after ``BreakerTuning.failure_threshold`` failures in a row."""


class NoProviderAvailable(Exception):
    """Raised by ``ProviderPool.call`` when every provider is cooling down or failed.

    A step catches it and uses its ``degrade`` fallback, or parks the item until ``retry_at`` (a Unix timestamp) when it has none.
    """

    def __init__(self, message: str, retry_at: float | None = None):
        super().__init__(message)
        self.retry_at = retry_at


class Park(Exception):
    """Raise inside a step to set the item aside until ``until``, a Unix timestamp.

    The item is stored as ``parked`` instead of failed. ``run_many(..., wait_for_parked=True)`` or ``WorkflowRecovery.resume()`` tries it again later. ``reason`` is recorded in the audit trail.

    Example::

        raise Park(time.time() + 30, "waiting on a file")
    """

    def __init__(self, until: float, reason: str = ""):
        super().__init__(reason or "parked")
        self.until = until
        self.reason = reason


class StepFailed(Exception):
    """A step failed after its retries. Recorded as the item's error.

    ``cause`` is the original exception. ``transient`` is true when the step ran out of retries or raised a ``TransientError``, and false when it raised an error its policy does not retry.
    """

    def __init__(self, step_name: str, cause: BaseException, transient: bool):
        super().__init__(f"{step_name}: {describe_error(cause)}")
        self.step_name = step_name
        self.cause = cause
        self.transient = transient


class PipelineHalted(Exception):
    """Base class for errors that stop a whole ``run_many`` call. Catch this to handle both halt cases."""


class CriticalStepFailed(PipelineHalted):
    """A critical step ran out of retries (or raised ``TransientError``), so the run was halted.

    In-flight items are cancelled and the run is stored as halted. Running the batch again continues once the step's health check passes.
    """

    def __init__(self, step_name: str, item_key: str, cause: BaseException):
        super().__init__(
            f"critical step {step_name} failed for {item_key}: {describe_error(cause)}"
        )
        self.step_name = step_name
        self.item_key = item_key
        self.cause = cause


class HealthCheckFailed(PipelineHalted):
    """``run_many`` refused to continue a halted run because the critical step's health check still fails."""

    def __init__(self, step_name: str, reason: str):
        super().__init__(
            f"the run is halted and the resource behind {step_name} is still unhealthy: {reason}"
        )
        self.step_name = step_name
        self.reason = reason


def describe_error(error: BaseException) -> str:
    """Return a short message for ``error``: its text, or its type name when it has none."""
    if isinstance(error, TimeoutError):
        return "timed out"
    return str(error) or type(error).__name__
