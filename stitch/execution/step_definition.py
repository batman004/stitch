from dataclasses import dataclass
from typing import Any, Callable

from ..retry_policy import RetryPolicy


@dataclass(frozen=True)
class StepDefinition:
    name: str
    function: Callable[..., Any]
    retry_policy: RetryPolicy
    timeout_seconds: float | None
    degrade_function: Callable[..., Any] | None
