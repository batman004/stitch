import inspect
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunStatistics:
    """Counts for one ``run_many`` call, returned when it finishes.

    ``items_skipped`` were already done before this call. ``items_degraded`` finished using at least one fallback. ``parked_remaining`` are still parked at the end. ``steps_skipped`` reused a checkpoint instead of running. ``failovers`` counts calls a ``ProviderPool`` answered with a provider other than the first one it tried.
    """

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
    """Receives live events from ``run_many``. Subclass it and add ``on_<event>`` methods for the events you want.

    Event names match the audit trail, for example ``step_retry``, ``item_failed`` or ``failover``. Each handler gets only the keyword arguments it names, such as ``item``, ``step``, ``attempt``, ``error`` or ``delay``; add ``**data`` to receive all of them.

    Example::

        class LineReporter(Reporter):
            def on_step_retry(self, step, attempt, error, delay):
                print(f"{step} attempt {attempt} failed ({error}); retry in {delay:.2f}s")
    """

    def emit(self, event: str, **data: Any) -> None:
        """Call ``on_<event>`` with the fields it accepts; do nothing if there is no such method."""
        handler = getattr(self, f"on_{event}", None)
        if handler is None:
            return
        handler(**self._arguments_the_handler_accepts(handler, data))

    def _arguments_the_handler_accepts(self, handler, data: dict) -> dict:
        parameters = inspect.signature(handler).parameters.values()
        if any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
        ):
            return data
        accepted_names = {parameter.name for parameter in parameters}
        return {name: value for name, value in data.items() if name in accepted_names}
