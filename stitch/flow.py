"""The decorator API: turn any long-running async function into a durable one.

    import stitch

    stitch.configure("state.db")

    @stitch.step(retry=stitch.Retry(attempts=5))
    async def charge(order): ...

    @stitch.workflow(key=lambda order: order["id"])
    async def fulfil(order):
        await validate(order)
        await charge(order)
        return await ship(order)

    await fulfil(order)             # durable call: same key twice -> second call is a no-op
    await fulfil.map(orders, concurrency=20)
    await stitch.resume()             # after a crash: continue everything unfinished
    await stitch.heal()               # after providers recover: redo degraded steps

A workflow takes ONE argument, the unit of work, and it must be JSON-serialisable.
The engine stores it so that `resume()` can restart the work after a crash with
no help from the caller, which is what makes this work without a queue in front.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any

from .engine import NO_RETRY, Crash, Reporter, Retry, Stats, run_batch
from .errors import WorkflowFailed, WorkflowParked
from .store import NullStore, Store

_registry: dict[str, Workflow] = {}
_defaults: dict[str, Any] = {"store": None, "reporter": None}


def configure(
    store: str | Path | Store | None = None, *, reporter: Reporter | None = None
) -> Store:
    """Choose where checkpoints live. Call once at start-up."""
    if store is not None:
        _defaults["store"] = store if isinstance(store, Store) else Store(store)
    if reporter is not None:
        _defaults["reporter"] = reporter
    return _store(None)


def _store(store: Store | None) -> Store:
    store = store or _defaults["store"]
    if store is None:
        raise RuntimeError(
            "no store configured: call stitch.configure('state.db') first"
        )
    return store


def default_key(value: Any) -> str:
    if isinstance(value, dict) and "id" in value:
        return str(value["id"])
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


class Workflow:
    def __init__(
        self,
        fn: Callable[[Any], Awaitable[Any]],
        *,
        name: str,
        key: Callable[[Any], str],
        batch: str,
        item_retry: Retry,
        concurrency: int,
    ):
        self.fn, self.name, self.key = fn, name, key
        self.batch, self.item_retry, self.concurrency = batch, item_retry, concurrency
        self.__wrapped__ = fn
        self.__doc__ = fn.__doc__
        _registry[name] = self

    # -- running ----------------------------------------------------------

    async def map(
        self,
        items: Iterable[Any] | AsyncIterable[Any],
        *,
        store: Store | None = None,
        batch: str | None = None,
        concurrency: int | None = None,
        reporter: Reporter | None = None,
        crash: Crash | None = None,
        wait_parked: bool | float = False,
        **options,
    ) -> Stats:
        """Run the workflow over many inputs. Finished items are skipped, unfinished ones resume."""
        options.setdefault("item_retry", self.item_retry)
        return await run_batch(
            self.name,
            items,
            self.fn,
            key=self.key,
            store=_store(store),
            batch=batch or self.batch,
            concurrency=concurrency or self.concurrency,
            reporter=reporter or _defaults["reporter"],
            crash=crash,
            wait_parked=wait_parked,
            **options,
        )

    async def __call__(
        self,
        value: Any,
        *,
        store: Store | None = None,
        batch: str | None = None,
        wait_parked: bool | float = True,
        **options,
    ) -> Any:
        """One durable call. Returns what the function returned, from the checkpoint if it
        already finished. Waits out cooldowns by default; raises WorkflowParked if told not to.
        """
        store = _store(store)
        batch = batch or self.batch
        stats = await self.map(
            [value], store=store, batch=batch, wait_parked=wait_parked, **options
        )
        row = store.item_row(f"{self.name}:{batch}", self.key(value))
        if row is None or row["status"] == "failed":
            raise WorkflowFailed(row["error"] if row else "workflow did not run")
        if row["status"] == "parked":
            raise WorkflowParked(row["retry_at"], row["error"] or "parked")
        return row["result"]

    async def resume(self, **kw) -> dict[str, Stats]:
        return await resume(workflows=[self.name], **kw)

    async def heal(self, **kw) -> dict[str, Stats]:
        return await heal(workflows=[self.name], **kw)


def workflow(
    fn=None,
    *,
    name: str | None = None,
    key: Callable[[Any], str] = default_key,
    batch: str = "default",
    item_retry: Retry = NO_RETRY,
    concurrency: int = 1,
):
    """Make an async function durable.

    key:         identifies one unit of work (default: its "id" field, or a hash of it).
                 The same key is never done twice.
    batch:       a label; (name, batch) is the run, so the same command lands on the same run.
    item_retry:  how many times to start the whole unit over after a transient step failure.
                 Leave at NO_RETRY: steps retry themselves, and checkpoints make a restart cheap.
    """

    def decorate(fn):
        return Workflow(
            fn,
            name=name or fn.__name__,
            key=key,
            batch=batch,
            item_retry=item_retry,
            concurrency=concurrency,
        )

    return decorate(fn) if fn is not None else decorate


# -- recovery --------------------------------------------------------------


async def resume(
    *,
    store: Store | None = None,
    workflows: list[str] | None = None,
    reporter: Reporter | None = None,
    concurrency: int = 1,
    wait_parked: bool | float = False,
) -> dict[str, Stats]:
    """Continue everything that was interrupted or parked, with no caller involved.

    Call it at start-up, like a worker reconnecting to Temporal. Each unfinished item
    restarts from its stored input and skips every step that already has a checkpoint.
    """
    store = _store(store)
    out: dict[str, Stats] = {}
    for run_id, wf_name, _ in store.runs():
        wf = _registry.get(wf_name)
        if wf is None or (workflows is not None and wf_name not in workflows):
            continue
        values = [value for _, value in store.resumable(run_id)]
        if not values:
            continue
        batch = run_id.split(":", 1)[1]
        out[run_id] = await wf.map(
            values,
            store=store,
            batch=batch,
            concurrency=concurrency,
            reporter=reporter,
            wait_parked=wait_parked,
        )
    return out


async def heal(
    *,
    store: Store | None = None,
    workflows: list[str] | None = None,
    reporter: Reporter | None = None,
    concurrency: int = 1,
    wait_parked: bool | float = False,
) -> dict[str, Stats]:
    """Redo steps that finished with a degraded (fallback) result.

    The first degraded step of each item and everything after it is forgotten and run
    again. If the providers are still down the steps degrade again, so it is safe to
    call repeatedly, for example from a timer. Steps after a degraded one run again
    because they consumed its placeholder output, so they must be idempotent
    (publishing the same item twice must overwrite, not duplicate).
    """
    store = _store(store)
    out: dict[str, Stats] = {}
    for run_id, wf_name, _ in store.runs():
        wf = _registry.get(wf_name)
        if wf is None or (workflows is not None and wf_name not in workflows):
            continue
        degraded = store.degraded_items(run_id)
        if not degraded:
            continue
        for item, _ in degraded:
            redone = store.invalidate_degraded(run_id, item)
            store.audit(
                run_id, "heal_started", {"item": item, "redo": ",".join(redone)}
            )
        batch = run_id.split(":", 1)[1]
        out[run_id] = await wf.map(
            [value for _, value in degraded],
            store=store,
            batch=batch,
            concurrency=concurrency,
            reporter=reporter,
            wait_parked=wait_parked,
        )
    return out


__all__ = [
    "Workflow",
    "configure",
    "default_key",
    "heal",
    "resume",
    "workflow",
    "NullStore",
]
