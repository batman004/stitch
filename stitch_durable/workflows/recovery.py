from typing import Iterator

from ..alerts import Alerter
from ..execution.reporting import Reporter, RunStatistics
from ..storage.checkpoint_store import CheckpointStore
from .registry import WorkflowRegistry
from .workflow import Workflow


class WorkflowRecovery:
    """Pick up earlier runs stored in a ``CheckpointStore`` without passing the items again.

    ``resume()`` continues items left running or parked. ``heal()`` redoes items that finished with a fallback result. Both look workflows up by name, so import the modules that define them first; runs whose workflow is not registered are ignored.
    """

    def __init__(
        self, store: CheckpointStore, registry: WorkflowRegistry | None = None
    ):
        """``store`` holds the runs to recover. ``registry`` defaults to ``WorkflowRegistry.default()``."""
        self.store = store
        self.registry = registry or WorkflowRegistry.default()

    async def resume(
        self,
        workflow_names: list[str] | None = None,
        concurrency: int = 1,
        reporter: Reporter | None = None,
        alerter: Alerter | None = None,
        wait_for_parked: bool = False,
        maximum_parked_wait_seconds: float | None = None,
    ) -> dict[str, RunStatistics]:
        """Continue every item that was interrupted (still ``running``) or ``parked``.

        Finished steps are reused. Failed and done items are left alone. ``workflow_names`` limits the runs to those workflows; by default every registered workflow is checked. The other arguments are passed to ``Workflow.run_many``.

        Returns the statistics of each run that had work, keyed by run id (``"name:batch_label"``).
        """
        outcomes = {}
        for run_id, workflow in self._registered_runs(workflow_names):
            inputs = [
                item.input_value for item in self.store.items.list_resumable(run_id)
            ]
            if inputs:
                outcomes[run_id] = await self._rerun(
                    run_id,
                    workflow,
                    inputs,
                    concurrency,
                    reporter,
                    alerter,
                    wait_for_parked,
                    maximum_parked_wait_seconds,
                )
        return outcomes

    async def heal(
        self,
        workflow_names: list[str] | None = None,
        concurrency: int = 1,
        reporter: Reporter | None = None,
        alerter: Alerter | None = None,
        wait_for_parked: bool = False,
        maximum_parked_wait_seconds: float | None = None,
    ) -> dict[str, RunStatistics]:
        """Redo every item that used a ``degrade`` fallback, now that the real dependency may be back.

        For each such item, the checkpoints from the first degraded step onward are deleted and the item runs again; steps before it are reused. If the dependency is still down, the fallback is used again. ``workflow_names`` limits the runs to those workflows; by default every registered workflow is checked. The other arguments are passed to ``Workflow.run_many``.

        Returns the statistics of each run that had degraded items, keyed by run id (``"name:batch_label"``).
        """
        outcomes = {}
        for run_id, workflow in self._registered_runs(workflow_names):
            degraded_items = self.store.items.list_degraded(run_id)
            if degraded_items:
                self._invalidate_degraded_items(run_id, degraded_items)
                inputs = [item.input_value for item in degraded_items]
                outcomes[run_id] = await self._rerun(
                    run_id,
                    workflow,
                    inputs,
                    concurrency,
                    reporter,
                    alerter,
                    wait_for_parked,
                    maximum_parked_wait_seconds,
                )
        return outcomes

    def _invalidate_degraded_items(self, run_id: str, degraded_items: list) -> None:
        for item in degraded_items:
            redone_steps = self.store.invalidate_degraded_steps(run_id, item.item_key)
            self.store.audit.record(
                run_id,
                "heal_started",
                {"item": item.item_key, "redo": ",".join(redone_steps)},
            )

    async def _rerun(
        self,
        run_id: str,
        workflow: Workflow,
        inputs: list,
        concurrency: int,
        reporter,
        alerter,
        wait_for_parked: bool,
        maximum_parked_wait_seconds: float | None,
    ) -> RunStatistics:
        batch_label = run_id.split(":", 1)[1]
        return await workflow.run_many(
            inputs,
            self.store,
            batch_label=batch_label,
            concurrency=concurrency,
            reporter=reporter,
            alerter=alerter,
            wait_for_parked=wait_for_parked,
            maximum_parked_wait_seconds=maximum_parked_wait_seconds,
        )

    def _registered_runs(
        self, workflow_names: list[str] | None
    ) -> Iterator[tuple[str, Workflow]]:
        for run_id, workflow_name, _ in self.store.runs.list_runs():
            workflow = self.registry.find(workflow_name)
            if workflow is None:
                continue
            if workflow_names is not None and workflow_name not in workflow_names:
                continue
            yield run_id, workflow
