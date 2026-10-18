import contextvars
from dataclasses import dataclass
from typing import Any

from ..storage.checkpoint_store import CheckpointStore


@dataclass(frozen=True)
class StepIdentity:
    item_key: str
    step_id: str
    sequence: int


@dataclass(frozen=True)
class StepContext:
    run_id: str
    item_key: str
    step_name: str
    attempt: int


class ItemContext:
    def __init__(self, item_key: str):
        self.item_key = item_key
        self.degraded = False
        self._sequence = 0
        self._call_counts: dict[str, int] = {}

    def register_step_call(self, step_name: str) -> StepIdentity:
        call_count = self._call_counts.get(step_name, 0) + 1
        self._call_counts[step_name] = call_count
        self._sequence += 1
        step_id = step_name if call_count == 1 else f"{step_name}#{call_count}"
        return StepIdentity(self.item_key, step_id, self._sequence)


class ExecutionContext:
    """Tracks the active run, item and step for the current async task.

    The engine binds and releases these values; user code normally only reads them, for example ``ExecutionContext.current_step()`` to log the attempt number. Outside a workflow every ``current_*`` method returns ``None``.
    """

    _run = contextvars.ContextVar("stitch_run", default=None)
    _item = contextvars.ContextVar("stitch_item", default=None)
    _step = contextvars.ContextVar("stitch_step", default=None)

    @classmethod
    def bind_run(cls, session) -> contextvars.Token:
        return cls._run.set(session)

    @classmethod
    def release_run(cls, token: contextvars.Token) -> None:
        cls._run.reset(token)

    @classmethod
    def bind_item(cls, item_context: ItemContext) -> contextvars.Token:
        return cls._item.set(item_context)

    @classmethod
    def release_item(cls, token: contextvars.Token) -> None:
        cls._item.reset(token)

    @classmethod
    def bind_step(cls, step_context: StepContext) -> contextvars.Token:
        return cls._step.set(step_context)

    @classmethod
    def release_step(cls, token: contextvars.Token) -> None:
        cls._step.reset(token)

    @classmethod
    def current_run(cls):
        """Return the active run session, or ``None`` outside a workflow."""
        return cls._run.get()

    @classmethod
    def current_item(cls) -> ItemContext | None:
        """Return the item being processed (``item_key``, ``degraded``), or ``None``."""
        return cls._item.get()

    @classmethod
    def current_step(cls) -> StepContext | None:
        """Return the running step (``run_id``, ``item_key``, ``step_name``, ``attempt``), or ``None``."""
        return cls._step.get()

    @classmethod
    def require_store(cls) -> CheckpointStore:
        """Return the active run's ``CheckpointStore``; raise ``RuntimeError`` outside a workflow."""
        session = cls._run.get()
        if session is None:
            raise RuntimeError("this call is only valid inside a running workflow")
        return session.store

    @classmethod
    def report_event(cls, event: str, **data: Any) -> None:
        """Send ``event`` to the run's audit trail and reporter, adding the current item and step. Does nothing outside a workflow."""
        session = cls._run.get()
        if session is None:
            return
        item_context = cls._item.get()
        step_context = cls._step.get()
        if item_context is not None:
            data.setdefault("item", item_context.item_key)
        if step_context is not None:
            data.setdefault("step", step_context.step_name)
            data.setdefault("attempt", step_context.attempt)
        session.emit(event, **data)
