import hashlib
import json
from typing import Any, Awaitable, Callable, Iterable, AsyncIterable

from ..execution.batch_runner import BatchOptions, BatchRunner
from ..execution.reporting import Reporter, RunStatistics
from ..retry_policy import NO_RETRY, RetryPolicy
from ..storage.checkpoint_store import CheckpointStore
from .registry import WorkflowRegistry

DEFAULT_BATCH_LABEL = "default"


def default_item_key(value: Any) -> str:
    if isinstance(value, dict) and "id" in value:
        return str(value["id"])
    encoded = json.dumps(value, sort_keys=True, default=str).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


class Workflow:
    def __init__(
        self,
        function: Callable[[Any], Awaitable[Any]],
        name: str | None = None,
        key_function: Callable[[Any], str] = default_item_key,
        batch_label: str = DEFAULT_BATCH_LABEL,
        item_retry_policy: RetryPolicy = NO_RETRY,
        concurrency: int = 1,
        registry: WorkflowRegistry | None = None,
    ):
        self.function = function
        self.name = name or function.__name__
        self.key_function = key_function
        self.batch_label = batch_label
        self.item_retry_policy = item_retry_policy
        self.concurrency = concurrency
        (registry or WorkflowRegistry.default()).register(self)

    async def run_many(
        self,
        items: Iterable[Any] | AsyncIterable[Any],
        store: CheckpointStore,
        batch_label: str | None = None,
        concurrency: int | None = None,
        reporter: Reporter | None = None,
        wait_for_parked: bool = False,
        maximum_parked_wait_seconds: float | None = None,
        parameters: dict | None = None,
    ) -> RunStatistics:
        options = BatchOptions(
            concurrency=concurrency or self.concurrency,
            item_retry_policy=self.item_retry_policy,
            wait_for_parked=wait_for_parked,
            maximum_parked_wait_seconds=maximum_parked_wait_seconds,
            parameters=parameters,
        )
        runner = BatchRunner(
            self.name, batch_label or self.batch_label, self.function, self.key_function, store, reporter, options
        )
        return await runner.run(items)


def workflow(
    function: Callable[[Any], Awaitable[Any]] | None = None,
    *,
    name: str | None = None,
    key: Callable[[Any], str] = default_item_key,
    batch: str = DEFAULT_BATCH_LABEL,
    item_retry: RetryPolicy = NO_RETRY,
    concurrency: int = 1,
):
    def decorate(target: Callable[[Any], Awaitable[Any]]) -> Workflow:
        return Workflow(target, name, key, batch, item_retry, concurrency)

    if function is None:
        return decorate
    return decorate(function)
