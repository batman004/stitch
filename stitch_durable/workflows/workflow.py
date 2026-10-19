import hashlib
import json
from typing import Any, Awaitable, Callable, Iterable, AsyncIterable

from ..execution.batch_runner import BatchOptions, BatchRunner
from ..alerts import Alerter
from ..execution.reporting import Reporter, RunStatistics
from ..execution.step_decorator import require_async_function
from ..retry_policy import NO_RETRY, RetryPolicy
from ..storage.checkpoint_store import CheckpointStore
from .registry import WorkflowRegistry

DEFAULT_BATCH_LABEL = "default"


def default_item_key(value: Any) -> str:
    """Return ``value["id"]`` for a dict with an ``id``, otherwise a short hash of the value."""
    if isinstance(value, dict) and "id" in value:
        return str(value["id"])
    encoded = json.dumps(value, sort_keys=True, default=str).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


class Workflow:
    """A durable batch job: an async function that handles one item, run over many items.

    The function calls ``@step`` functions. Inside ``run_many`` each step result is checkpointed, so a rerun skips finished items and reuses finished steps.

    Example::

        async def process(article):
            text = await download(article)
            return await summarise(article, text)

        store = CheckpointStore("state.db")
        stats = await Workflow(process, name="articles").run_many(articles, store)

    Creating a ``Workflow`` registers it by name, so ``WorkflowRecovery`` can find it later. The ``@workflow`` decorator builds the same object.
    """

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
        """Build and register a workflow.

        ``function`` is an ``async def`` that takes one item. ``name`` is the registry name and the first part of the run id; the function name is used when it is left out. ``key_function`` turns an item into a stable id; by default that is ``item["id"]`` for a dict with an ``id``, otherwise a short hash of the item. ``batch_label`` is the second part of the run id; items are tracked per ``name:batch_label``. ``item_retry_policy`` retries a whole item after a step fails; finished steps are kept. The default is no retry. ``concurrency`` is how many items run at once. ``registry`` defaults to ``WorkflowRegistry.default()``.
        """
        require_async_function(function, "Workflow")
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
        alerter: Alerter | None = None,
    ) -> RunStatistics:
        """Process every item, checkpointing each step, and return the run's counts.

        ``items`` can be a list, any iterable, or an async iterable. Each item and every step input and result must be JSON-serializable. Items already done in this run are skipped; failed and parked items are tried again; interrupted items continue from their last finished step.

        ``store`` is the ``CheckpointStore`` that holds the state. ``batch_label`` and ``concurrency`` override the values given to the constructor; use a new ``batch_label`` (for example today's date) to start a fresh run with the same workflow. ``reporter`` receives live events. ``wait_for_parked`` keeps the call running until parked items are due and retries them, for at most ``maximum_parked_wait_seconds`` when that is set. ``parameters`` is a dict stored with the run for your own reference. ``alerter`` is told when a critical step halts the run and when it recovers.

        Raises ``CriticalStepFailed`` when a critical step halts the run, and ``HealthCheckFailed`` when the run is halted and the critical step's health check still fails. An ordinary step failure does not raise: it marks that item failed and the batch continues.
        """
        options = BatchOptions(
            concurrency=concurrency or self.concurrency,
            item_retry_policy=self.item_retry_policy,
            wait_for_parked=wait_for_parked,
            maximum_parked_wait_seconds=maximum_parked_wait_seconds,
            parameters=parameters,
            alerter=alerter,
        )
        runner = BatchRunner(
            self.name,
            batch_label or self.batch_label,
            self.function,
            self.key_function,
            store,
            reporter,
            options,
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
    """Turn an async function that handles one item into a ``Workflow``.

    Write ``@workflow`` on its own, or pass options: ``@workflow(name=..., key=..., batch=..., item_retry=..., concurrency=...)``.

    The decorator does not run the function. It builds a ``Workflow``, registers it, and returns that object. A later resume looks the workflow up by name. Call ``run_many`` on the result to process a batch and write checkpoints.

    ``name`` is the registry name; the function name is used when it is left out. ``key`` builds a stable id for each item. By default that is ``item["id"]`` when the item is a dict with an ``id``, otherwise a short hash of the item. ``batch`` is the label stored with the run. ``item_retry`` says how to retry a whole item after a step failure; the default is no retry. ``concurrency`` is how many items to process at once.
    """

    def decorate(target: Callable[[Any], Awaitable[Any]]) -> Workflow:
        """Register ``target`` as a workflow using the options passed to ``@workflow``."""
        return Workflow(target, name, key, batch, item_retry, concurrency)

    if function is None:
        return decorate
    return decorate(function)
