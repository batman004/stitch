from typing import Iterator

from ..execution.reporting import Reporter, RunStatistics
from ..storage.checkpoint_store import CheckpointStore
from .registry import WorkflowRegistry
from .workflow import Workflow


class WorkflowRecovery:
    def __init__(self, store: CheckpointStore, registry: WorkflowRegistry | None = None):
        self.store = store
        self.registry = registry or WorkflowRegistry.default()

    async def resume(
        self,
        workflow_names: list[str] | None = None,
        concurrency: int = 1,
        reporter: Reporter | None = None,
        wait_for_parked: bool = False,
        maximum_parked_wait_seconds: float | None = None,
    ) -> dict[str, RunStatistics]:
        outcomes = {}
        for run_id, workflow in self._registered_runs(workflow_names):
            inputs = [item.input_value for item in self.store.items.list_resumable(run_id)]
            if inputs:
                outcomes[run_id] = await self._rerun(
                    run_id, workflow, inputs, concurrency, reporter, wait_for_parked, maximum_parked_wait_seconds
                )
        return outcomes

    async def heal(
        self,
        workflow_names: list[str] | None = None,
        concurrency: int = 1,
        reporter: Reporter | None = None,
        wait_for_parked: bool = False,
        maximum_parked_wait_seconds: float | None = None,
    ) -> dict[str, RunStatistics]:
        outcomes = {}
        for run_id, workflow in self._registered_runs(workflow_names):
            degraded_items = self.store.items.list_degraded(run_id)
            if degraded_items:
                self._invalidate_degraded_items(run_id, degraded_items)
                inputs = [item.input_value for item in degraded_items]
                outcomes[run_id] = await self._rerun(
                    run_id, workflow, inputs, concurrency, reporter, wait_for_parked, maximum_parked_wait_seconds
                )
        return outcomes

    def _invalidate_degraded_items(self, run_id: str, degraded_items: list) -> None:
        for item in degraded_items:
            redone_steps = self.store.invalidate_degraded_steps(run_id, item.item_key)
            self.store.audit.record(run_id, "heal_started", {"item": item.item_key, "redo": ",".join(redone_steps)})

    async def _rerun(
        self, run_id: str, workflow: Workflow, inputs: list, concurrency: int, reporter,
        wait_for_parked: bool, maximum_parked_wait_seconds: float | None,
    ) -> RunStatistics:
        batch_label = run_id.split(":", 1)[1]
        return await workflow.run_many(
            inputs, self.store, batch_label=batch_label, concurrency=concurrency, reporter=reporter,
            wait_for_parked=wait_for_parked, maximum_parked_wait_seconds=maximum_parked_wait_seconds,
        )

    def _registered_runs(self, workflow_names: list[str] | None) -> Iterator[tuple[str, Workflow]]:
        for run_id, workflow_name, _ in self.store.runs.list_runs():
            workflow = self.registry.find(workflow_name)
            if workflow is None:
                continue
            if workflow_names is not None and workflow_name not in workflow_names:
                continue
            yield run_id, workflow
