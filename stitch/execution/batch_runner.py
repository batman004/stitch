import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable

from ..retry_policy import NO_RETRY, RetryPolicy
from ..storage.checkpoint_store import CheckpointStore
from .context import ExecutionContext
from .item_processor import ItemProcessor
from .reporting import Reporter, RunStatistics
from .run_session import RunSession

PARKED_WAKE_UP_MARGIN_SECONDS = 0.05
WORKER_STOP_SIGNAL = object()


@dataclass(frozen=True)
class BatchOptions:
    concurrency: int = 1
    item_retry_policy: RetryPolicy = NO_RETRY
    wait_for_parked: bool = False
    maximum_parked_wait_seconds: float | None = None
    record_audit: bool = True
    parameters: dict | None = None


class BatchRunner:
    def __init__(
        self,
        workflow_name: str,
        batch_label: str,
        handler: Callable[[Any], Awaitable[Any]],
        key_function: Callable[[Any], str],
        store: CheckpointStore,
        reporter: Reporter | None = None,
        options: BatchOptions = BatchOptions(),
    ):
        self.workflow_name = workflow_name
        self.options = options
        self.store = store
        run_id = f"{workflow_name}:{batch_label}"
        self.session = RunSession(run_id, store, reporter or Reporter(), options.record_audit)
        self.processor = ItemProcessor(self.session, handler, key_function, options.item_retry_policy)

    async def run(self, items: Iterable[Any] | AsyncIterable[Any]) -> RunStatistics:
        previous_status = self.store.runs.open_run(
            self.session.run_id, self.workflow_name, self.options.parameters or {}
        )
        self.session.emit(
            "run_started", run_id=self.session.run_id, concurrency=self.options.concurrency,
            resume=self._summarize_previous_run(previous_status),
        )
        token = ExecutionContext.bind_run(self.session)
        try:
            await self._process_items(items)
            if self.options.wait_for_parked:
                await self._retry_parked_items()
        finally:
            ExecutionContext.release_run(token)
        return self._finish_run()

    async def _process_items(self, items: Iterable[Any] | AsyncIterable[Any]) -> None:
        concurrency = self.options.concurrency
        queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency)
        async with asyncio.TaskGroup() as workers:
            for _ in range(concurrency):
                workers.create_task(self._work_through(queue))
            async for item in self._as_async_iterator(items):
                await queue.put(item)
            for _ in range(concurrency):
                await queue.put(WORKER_STOP_SIGNAL)

    async def _work_through(self, queue: asyncio.Queue) -> None:
        while True:
            item = await queue.get()
            if item is WORKER_STOP_SIGNAL:
                return
            await self.processor.process(item)

    async def _retry_parked_items(self) -> None:
        deadline = self._parked_wait_deadline()
        while True:
            parked_items = self.store.items.list_parked(self.session.run_id)
            if not parked_items:
                return
            due_at = min(parked.retry_at for parked in parked_items)
            delay_seconds = max(0.0, due_at - time.time())
            if deadline is not None and time.time() + delay_seconds > deadline:
                return
            self.session.emit("cooldown_wait", count=len(parked_items), delay=delay_seconds, until=due_at)
            await asyncio.sleep(delay_seconds + PARKED_WAKE_UP_MARGIN_SECONDS)
            await self._process_due_items(parked_items)

    async def _process_due_items(self, parked_items: list) -> None:
        cutoff = time.time() + PARKED_WAKE_UP_MARGIN_SECONDS
        due_values = [parked.input_value for parked in parked_items if parked.retry_at <= cutoff]
        self.session.emit("cooldown_over", count=len(due_values))
        await self._process_items(due_values)

    def _parked_wait_deadline(self) -> float | None:
        limit = self.options.maximum_parked_wait_seconds
        if limit is None:
            return None
        return time.time() + limit

    def _finish_run(self) -> RunStatistics:
        statistics = self.session.statistics
        statistics.parked_remaining = len(self.store.items.list_parked(self.session.run_id))
        if statistics.items_failed == 0 and statistics.parked_remaining == 0:
            self.store.runs.finish_run(self.session.run_id)
        statistics.elapsed_seconds = time.monotonic() - statistics.started_at
        self.session.emit("run_finished", stats=statistics)
        return statistics

    def _summarize_previous_run(self, previous_status: str | None) -> dict | None:
        if previous_status is None:
            return None
        run_id = self.session.run_id
        statuses = [status for _, status in self.store.items.list_statuses(run_id)]
        interrupted = [
            (record.item_key, record.step_name)
            for record in self.store.steps.list_for_run(run_id)
            if record.status == "running"
        ]
        return {
            "previous": previous_status,
            "done": statuses.count("done"),
            "failed": statuses.count("failed"),
            "parked": statuses.count("parked"),
            "interrupted": interrupted,
        }

    async def _as_async_iterator(self, items: Iterable[Any] | AsyncIterable[Any]) -> AsyncIterator[Any]:
        if isinstance(items, AsyncIterable):
            async for item in items:
                yield item
            return
        for item in items:
            yield item
