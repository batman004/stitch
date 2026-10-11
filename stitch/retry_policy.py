import random
from dataclasses import dataclass

from .errors import TransientError


@dataclass(frozen=True)
class RetryPolicy:
    maximum_attempts: int = 4
    base_delay_seconds: float = 0.25
    maximum_delay_seconds: float = 4.0
    retry_on: tuple[type[BaseException], ...] = (TransientError, TimeoutError)

    def delay_before_next_attempt(self, attempt: int, error: BaseException) -> float:
        server_hint = getattr(error, "retry_after", None)
        if server_hint is not None:
            return float(server_hint)
        ceiling = min(self.maximum_delay_seconds, self.base_delay_seconds * 2 ** (attempt - 1))
        return ceiling / 2 + random.uniform(0, ceiling / 2)


DEFAULT_RETRY_POLICY = RetryPolicy()
NO_RETRY = RetryPolicy(maximum_attempts=1)
