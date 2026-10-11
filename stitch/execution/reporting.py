import inspect
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunStatistics:
    items_done: int = 0
    items_skipped: int = 0
    items_failed: int = 0
    items_parked: int = 0
    items_degraded: int = 0
    parked_remaining: int = 0
    steps_run: int = 0
    steps_skipped: int = 0
    steps_degraded: int = 0
    step_retries: int = 0
    item_retries: int = 0
    failovers: int = 0
    started_at: float = field(default_factory=time.monotonic)
    elapsed_seconds: float = 0.0


class Reporter:
    def emit(self, event: str, **data: Any) -> None:
        handler = getattr(self, f"on_{event}", None)
        if handler is None:
            return
        handler(**self._arguments_the_handler_accepts(handler, data))

    def _arguments_the_handler_accepts(self, handler, data: dict) -> dict:
        parameters = inspect.signature(handler).parameters.values()
        if any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters):
            return data
        accepted_names = {parameter.name for parameter in parameters}
        return {name: value for name, value in data.items() if name in accepted_names}
