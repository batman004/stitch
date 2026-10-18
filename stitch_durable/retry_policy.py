import random
from dataclasses import dataclass

from .errors import TransientError


@dataclass(frozen=True)
class RetryPolicy:
    """How often a step is tried and how long to wait between tries.

    ``maximum_attempts`` counts the first try. Only exceptions that are instances of a type in ``retry_on`` are retried; anything else fails the step at once. The wait doubles from ``base_delay_seconds`` up to ``maximum_delay_seconds``, with random jitter between 50% and 100% of that value. An exception with a ``retry_after`` attribute sets the wait instead.

    Example::

        @step(retry_policy=RetryPolicy(maximum_attempts=5, retry_on=(OSError, TimeoutError)))
        async def download(url): ...
    """

    maximum_attempts: int = 4
    base_delay_seconds: float = 0.25
    maximum_delay_seconds: float = 4.0
    retry_on: tuple[type[BaseException], ...] = (TransientError, TimeoutError)

    def delay_before_next_attempt(self, attempt: int, error: BaseException) -> float:
        """Return the seconds to wait after failed try number ``attempt`` (starting at 1)."""
        server_hint = getattr(error, "retry_after", None)
        if server_hint is not None:
            return float(server_hint)
        ceiling = min(
            self.maximum_delay_seconds, self.base_delay_seconds * 2 ** (attempt - 1)
        )
        return ceiling / 2 + random.uniform(0, ceiling / 2)


DEFAULT_RETRY_POLICY = RetryPolicy()
"""Four tries, retrying ``TransientError`` and ``TimeoutError``. Used by ``@step`` when no policy is given."""

NO_RETRY = RetryPolicy(maximum_attempts=1)
"""A single try. The default ``item_retry_policy`` of a ``Workflow``."""
