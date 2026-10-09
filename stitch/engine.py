"""The durable execution engine.

    @step(retry=Retry(attempts=4), timeout=5)
    async def capture_payment(order): ...

    async def fulfil(order):
        await validate(order)
        await capture_payment(order)
        ...

    await run_batch("fulfilment", orders, fulfil, key=lambda o: o["id"], store=Store(path))

(`workflow.py` wraps this in a decorator; this is the machinery underneath.)

Before a step runs, the engine looks up (run, item, step) in the store. If the
step already finished, its saved result is returned without calling the
function. Otherwise the function runs (with retries and a timeout) and its
result is saved before the next step starts. Kill the process at any point and
the next run skips everything that was saved.

When a step can't run because every provider is cooling down, the engine does
one of two things: use the step's `degrade` function to produce a placeholder
result and carry on (the item is flagged degraded and can be healed later), or
park the item until the cooldown ends while the rest of the batch continues.

Every event is also appended to an audit table, so any item's story can be
replayed with `trace`.

The one thing checkpoints cannot fix: a crash after a step's side effect
happened but before its checkpoint was written. That step runs again on resume,
so steps must be safe to repeat. `idempotency_key()` gives each step a key that
stays the same across retries and resumes, for APIs that deduplicate on it.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import inspect
import os
import random
import signal
import sys
import time
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from .errors import (
    NoProviderAvailable,
    Park,
    PermanentError,
    StepFailed,
    TransientError,
    describe,
)
from .store import NullStore, Store, dumps


# -- retry policy ----------------------------------------------------------

@dataclass(frozen=True)
class Retry:
    attempts: int = 4
    base: float = 0.25
    cap: float = 4.0
    retry_on: tuple[type[BaseException], ...] = (TransientError, TimeoutError)

    def delay(self, attempt: int, exc: BaseException) -> float:
        hint = getattr(exc, "retry_after", None)
        if hint is not None:  # the server told us how long to back off
            return float(hint)
        ceiling = min(self.cap, self.base * 2 ** (attempt - 1))
        return ceiling / 2 + random.uniform(0, ceiling / 2)


NO_RETRY = Retry(attempts=1)


# -- crash injection -------------------------------------------------------

@dataclass(frozen=True)
class Crash:
    """Kill the process with SIGKILL at a chosen point, like an OOM-killed pod.

    phase "before": the step has started but done nothing yet.
    phase "after":  the step's side effect happened, its checkpoint was not written.

    It fires only the first time that step starts, so re-running the exact
    same command resumes instead of crashing again.
    """

    item: str
    step: str
    phase: str = "before"

    @classmethod
    def parse(cls, spec: str) -> Crash:
        parts = spec.split(":")
        if len(parts) not in (2, 3) or (len(parts) == 3 and parts[2] not in ("before", "after")):
            raise ValueError(f"expected ITEM:STEP[:before|after], got {spec!r}")
        return cls(*parts)


def die() -> None:
    sys.stdout.flush()
    sys.stderr.flush()
    kill = getattr(signal, "SIGKILL", None)
    if kill is None:  # Windows
        os._exit(137)
    os.kill(os.getpid(), kill)


# -- context ---------------------------------------------------------------

@dataclass(frozen=True)
class StepContext:
    run_id: str
    item: str
    step: str
    attempt: int
    idempotent: bool

    @property
    def key(self) -> str:
        return f"{self.run_id}/{self.item}/{self.step}"


@dataclass
class _ItemContext:
    key: str
    seq: int = 0
    calls: dict[str, int] = field(default_factory=dict)
    degraded: bool = False

    def next_step(self, name: str) -> tuple[str, int]:
        # A step called twice in one item gets a distinct id: name, name#2, ...
        n = self.calls[name] = self.calls.get(name, 0) + 1
        self.seq += 1
        return (name if n == 1 else f"{name}#{n}"), self.seq


_run_var: contextvars.ContextVar[_Run] = contextvars.ContextVar("stitch_run")
_item_var: contextvars.ContextVar[_ItemContext] = contextvars.ContextVar("stitch_item")
_step_var: contextvars.ContextVar[StepContext | None] = contextvars.ContextVar("stitch_step", default=None)

_detached_store = NullStore()  # used by a Pool called outside any run


def current_step() -> StepContext | None:
    return _step_var.get()


def current_store() -> Store:
    run = _run_var.get(None)
    return run.store if run is not None else _detached_store


def idempotency_key() -> str | None:
    """Stable key for the running step, or None when idempotency is off."""
    ctx = _step_var.get()
    return ctx.key if ctx is not None and ctx.idempotent else None


def emit_current(event: str, **data: Any) -> None:
    """Report an event from inside a step (used by Pool). No-op outside a run."""
    run = _run_var.get(None)
    if run is None:
        return
    item = _item_var.get(None)
    ctx = _step_var.get()
    if item is not None:
        data.setdefault("item", item.key)
    if ctx is not None:
        data.setdefault("step", ctx.step)
        data.setdefault("attempt", ctx.attempt)
    run.emit(event, **data)


# -- the decorator ---------------------------------------------------------

def step(fn=None, *, retry: Retry = Retry(), timeout: float | None = None,
         name: str | None = None, degrade: Callable[..., Any] | None = None):
    """Mark an async function as a checkpointed step.

    degrade: called with the same arguments when the step cannot succeed because
    no provider is available or retries ran out. Its return value becomes the
    step's (degraded) result, so downstream steps and consumers get something
    instead of nothing. `heal()` redoes degraded steps once providers recover.
    If it declares a `cause` keyword it receives the exception (a
    NoProviderAvailable carries `retry_at`); it may raise Park to wait instead.
    """

    def decorate(fn: Callable[..., Awaitable[Any]]):
        step_name = name or fn.__name__

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            run = _run_var.get(None)
            if run is None:  # called outside a workflow: just a function
                return await fn(*args, **kwargs)
            step_id, seq = _item_var.get().next_step(step_name)
            return await run.execute(step_id, seq, fn, args, kwargs, retry, timeout, degrade)

        wrapper.step_name = step_name
        return wrapper

    return decorate(fn) if fn is not None else decorate


def _with_cause(degrade: Callable[..., Any], kwargs: dict, cause: BaseException | None) -> dict:
    try:
        wants_cause = "cause" in inspect.signature(degrade).parameters
    except (TypeError, ValueError):
        wants_cause = False
    return {**kwargs, "cause": cause} if wants_cause else kwargs


# -- the runner ------------------------------------------------------------

@dataclass
class Stats:
    items_done: int = 0
    items_skipped: int = 0
    items_failed: int = 0
    items_parked: int = 0      # times an item was set aside
    items_degraded: int = 0
    parked_left: int = 0       # still parked when the run returned
    steps_run: int = 0
    steps_skipped: int = 0
    steps_degraded: int = 0
    retries: int = 0
    item_retries: int = 0
    failovers: int = 0
    started: float = field(default_factory=time.monotonic)
    elapsed: float = 0.0


class Reporter:
    """Receives engine events. Subclass and define on_<event> methods.

    A handler only receives the fields it declares (or all of them if it takes
    **kwargs), so adding a field to an event never breaks existing reporters.
    """

    def emit(self, event: str, **data: Any) -> None:
        handler = getattr(self, f"on_{event}", None)
        if handler is None:
            return
        params = inspect.signature(handler).parameters.values()
        if not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params):
            names = {p.name for p in params}
            data = {k: v for k, v in data.items() if k in names}
        handler(**data)


_DONE = object()


@dataclass
class _Run:
    run_id: str
    store: Store
    handler: Callable[[Any], Awaitable[Any]]
    key: Callable[[Any], str]
    reporter: Reporter
    step_retries: bool
    item_retry: Retry
    idempotency: bool
    crash: Crash | None
    audit: bool
    stats: Stats = field(default_factory=Stats)

    def emit(self, event: str, **data: Any) -> None:
        if event == "failover":
            self.stats.failovers += 1
        if self.audit:
            self.store.audit(self.run_id, event, data)
        self.reporter.emit(event, **data)

    def _maybe_crash(self, item: str, step_id: str, phase: str, first_time: bool) -> None:
        c = self.crash
        if c and first_time and (c.item, c.step, c.phase) == (item, step_id, phase):
            self.emit("crash", item=item, step=step_id, phase=phase)
            die()

    async def execute(self, step_id, seq, fn, args, kwargs, retry: Retry, timeout, degrade):
        ictx = _item_var.get()
        item = ictx.key
        prev = self.store.get_step(self.run_id, item, step_id)
        if prev is not None and prev.status in ("done", "degraded"):
            if prev.status == "degraded":
                ictx.degraded = True
            self.stats.steps_skipped += 1
            self.emit("step_skipped", item=item, step=step_id, result=prev.result,
                      degraded=prev.status == "degraded")
            return prev.result

        first_time = prev is None
        self.store.step_started(self.run_id, item, step_id, seq)
        self.emit("step_started", item=item, step=step_id,
                  interrupted=prev is not None and prev.status == "running")
        self._maybe_crash(item, step_id, "before", first_time)

        policy = retry if self.step_retries else NO_RETRY
        t0 = time.monotonic()
        attempt = 0
        degraded_reason: str | None = None
        degraded_cause: BaseException | None = None
        while True:
            attempt += 1
            token = _step_var.set(StepContext(self.run_id, item, step_id, attempt, self.idempotency))
            try:
                async with asyncio.timeout(timeout):
                    result = await fn(*args, **kwargs)
                break
            except NoProviderAvailable as exc:
                if degrade is None:
                    until = exc.retry_at or time.time() + 5.0
                    self._wait(item, step_id, exc, attempt)
                    raise Park(until, describe(exc)) from exc
                degraded_reason, degraded_cause = describe(exc), exc
                break
            except Park as exc:
                self._wait(item, step_id, exc, attempt)
                raise
            except policy.retry_on as exc:
                if attempt >= policy.attempts:
                    if degrade is not None:
                        degraded_reason = f"retries exhausted: {describe(exc)}"
                        degraded_cause = exc
                        break
                    self._fail(item, step_id, exc, attempt)
                    raise StepFailed(step_id, exc, transient=True) from exc
                delay = policy.delay(attempt, exc)
                self.stats.retries += 1
                self.emit("step_retry", item=item, step=step_id, attempt=attempt,
                          attempts=policy.attempts, delay=delay, error=describe(exc))
                await asyncio.sleep(delay)
            except Exception as exc:
                self._fail(item, step_id, exc, attempt)
                raise StepFailed(step_id, exc, transient=isinstance(exc, TransientError)) from exc
            finally:
                _step_var.reset(token)

        if degraded_reason is not None:
            try:
                result = degrade(*args, **_with_cause(degrade, kwargs, degraded_cause))
                if inspect.isawaitable(result):
                    result = await result
            except Park as exc:  # the fallback itself chose to wait instead
                self._wait(item, step_id, exc, attempt)
                raise
            ictx.degraded = True
            self.stats.steps_degraded += 1
            self.emit("step_degraded", item=item, step=step_id, reason=degraded_reason)

        self._maybe_crash(item, step_id, "after", first_time)
        self.store.step_done(self.run_id, item, step_id, result, attempt,
                             degraded=degraded_reason is not None)
        self.stats.steps_run += 1
        self.emit("step_done", item=item, step=step_id, result=result, attempts=attempt,
                  elapsed=time.monotonic() - t0, degraded=degraded_reason is not None)
        return result

    def _fail(self, item, step_id, exc, attempts) -> None:
        self.store.step_failed(self.run_id, item, step_id, describe(exc), attempts)
        self.emit("step_failed", item=item, step=step_id, error=describe(exc), attempts=attempts)

    def _wait(self, item, step_id, exc, attempts) -> None:
        self.store.step_waiting(self.run_id, item, step_id, describe(exc), attempts)
        self.emit("step_waiting", item=item, step=step_id, reason=describe(exc))

    async def process(self, value: Any) -> Any:
        key = self.key(value)
        row = self.store.item_row(self.run_id, key)
        status = row["status"] if row else None
        if status == "done":
            self.stats.items_skipped += 1
            self.emit("item_skipped", item=key, degraded=row["degraded"])
            return row["result"]

        payload = dumps(value)
        attempt = 0
        while True:
            attempt += 1
            ictx = _ItemContext(key)
            token = _item_var.set(ictx)
            self.store.set_item(self.run_id, key, "running", input=payload,
                                retry_at=None, error=None)
            self.emit("item_started", item=key, attempt=attempt, resumed=status is not None,
                      was=status)
            t0 = time.monotonic()
            try:
                result = await self.handler(value)
            except Park as exc:
                if payload is None:  # can't be revived later without its input
                    self._item_failed(key, f"cannot park, input is not JSON: {exc.reason}")
                    return None
                self.store.set_item(self.run_id, key, "parked", retry_at=exc.until, error=exc.reason)
                self.stats.items_parked += 1
                self.emit("item_parked", item=key, until=exc.until, reason=exc.reason)
                return None
            except StepFailed as exc:
                if exc.transient and attempt < self.item_retry.attempts:
                    delay = self.item_retry.delay(attempt, exc.cause)
                    self.stats.item_retries += 1
                    self.emit("item_retry", item=key, attempt=attempt,
                              attempts=self.item_retry.attempts, delay=delay, error=str(exc))
                    await asyncio.sleep(delay)
                    continue
                self._item_failed(key, str(exc))
                return None
            except Exception as exc:  # a bug in the handler: fail this item, keep going
                self._item_failed(key, f"{type(exc).__name__}: {exc}")
                return None
            finally:
                _item_var.reset(token)
            self.store.set_item(self.run_id, key, "done", result=dumps(result),
                                degraded=int(ictx.degraded), retry_at=None, error=None)
            self.stats.items_done += 1
            if ictx.degraded:
                self.stats.items_degraded += 1
            self.emit("item_done", item=key, elapsed=time.monotonic() - t0, degraded=ictx.degraded)
            return result

    def _item_failed(self, key: str, error: str) -> None:
        self.store.set_item(self.run_id, key, "failed", error=error)
        self.stats.items_failed += 1
        self.emit("item_failed", item=key, error=error)

    async def worker(self, queue: asyncio.Queue) -> None:
        while (value := await queue.get()) is not _DONE:
            await self.process(value)


async def _drain(run: _Run, items, concurrency: int) -> None:
    """Push items through `concurrency` workers via a bounded queue.

    Items are pulled from the source only as fast as workers take them, so
    memory stays flat whether there are 10 items or 10 million.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency)
    async with asyncio.TaskGroup() as tg:  # holds a reference to every worker
        for _ in range(concurrency):
            tg.create_task(run.worker(queue))
        async for value in _aiter(items):
            await queue.put(value)
        for _ in range(concurrency):
            await queue.put(_DONE)


async def _wait_for_parked(run: _Run, concurrency: int, limit: float | None) -> None:
    """Sleep until parked items are due, then run them again. Repeat until none are left."""
    deadline = None if limit is None else time.time() + limit
    while parked := run.store.parked(run.run_id):
        due = min(retry_at for _, _, retry_at in parked)
        delay = max(0.0, due - time.time())
        if deadline is not None and time.time() + delay > deadline:
            break
        run.emit("cooldown_wait", count=len(parked), delay=delay, until=due)
        # The stored time is wall-clock and asyncio's timer is monotonic: wake a
        # little late, never early, or the breaker refuses the probe and we park again.
        await asyncio.sleep(delay + 0.05)
        now = time.time()
        ready = [value for _, value, retry_at in parked if retry_at <= now + 0.05]
        run.emit("cooldown_over", count=len(ready))
        await _drain(run, ready, concurrency)


async def run_batch(
    workflow: str,
    items: Iterable[Any] | AsyncIterable[Any],
    handler: Callable[[Any], Awaitable[Any]],
    *,
    key: Callable[[Any], str],
    store: Store,
    batch: str = "default",
    concurrency: int = 1,
    step_retries: bool | None = None,
    item_retry: Retry = NO_RETRY,
    idempotency: bool | None = None,
    crash: Crash | None = None,
    reporter: Reporter | None = None,
    params: dict | None = None,
    wait_parked: bool | float = False,
    audit: bool = True,
) -> Stats:
    """Run `handler` over every item, `concurrency` at a time.

    The run id is workflow:batch, so the same command always lands on the same
    run: a finished run skips everything, a crashed run resumes.

    wait_parked: after the batch, keep sleeping until parked items' cooldowns end
    and run them again. True waits as long as it takes; a number caps the wait
    in seconds (anything still parked stays parked in the store).
    """
    run = _Run(
        run_id=f"{workflow}:{batch}",
        store=store,
        handler=handler,
        key=key,
        reporter=reporter or Reporter(),
        step_retries=store.durable if step_retries is None else step_retries,
        item_retry=item_retry,
        idempotency=store.durable if idempotency is None else idempotency,
        crash=crash,
        audit=audit,
    )
    previous = store.open_run(run.run_id, workflow, params or {})
    run.emit("run_started", run_id=run.run_id, durable=store.durable,
             concurrency=concurrency, resume=_resume_summary(store, run.run_id, previous))

    token = _run_var.set(run)
    try:
        await _drain(run, items, concurrency)
        if wait_parked and store.durable:
            await _wait_for_parked(run, concurrency, None if wait_parked is True else float(wait_parked))
    finally:
        _run_var.reset(token)

    run.stats.parked_left = len(store.parked(run.run_id)) if store.durable else 0
    if run.stats.items_failed == 0 and run.stats.parked_left == 0:
        store.finish_run(run.run_id)
    run.stats.elapsed = time.monotonic() - run.stats.started
    run.emit("run_finished", stats=run.stats)
    return run.stats


def _resume_summary(store, run_id: str, previous: str | None) -> dict | None:
    if previous is None or not store.durable:
        return None
    items = store.items(run_id)
    interrupted = [(s.item, s.step) for s in store.steps(run_id) if s.status == "running"]
    return {
        "previous": previous,
        "done": sum(1 for _, st in items if st == "done"),
        "failed": sum(1 for _, st in items if st == "failed"),
        "parked": sum(1 for _, st in items if st == "parked"),
        "interrupted": interrupted,
    }


async def _aiter(items):
    if isinstance(items, AsyncIterable):
        async for value in items:
            yield value
    else:
        for value in items:
            yield value
