from pathlib import Path

from .audit_repository import AuditRepository
from .breaker_repository import BreakerRepository
from .database import Database
from .item_repository import ItemRepository
from .provider_repository import ProviderRepository
from .run_repository import RunRepository
from .step_repository import StepRepository


class CheckpointStore:
    """All workflow state in one SQLite file: runs, items, step checkpoints, audit trail and provider breakers.

    Example::

        store = CheckpointStore("state.db")
        stats = await workflow.run_many(items, store)
        print(store.items.get("name:default", "item-key").status)
        store.close()

    The tables are reached through ``runs``, ``items``, ``steps``, ``audit``, ``providers`` and ``breakers``. Use one store per process; async workers share its connection.
    """

    def __init__(self, path: str | Path):
        """Open or create the SQLite file at ``path`` (in WAL mode) and its tables."""
        self.database = Database(path)
        self.runs = RunRepository(self.database)
        self.items = ItemRepository(self.database)
        self.steps = StepRepository(self.database)
        self.audit = AuditRepository(self.database)
        self.providers = ProviderRepository(self.database)
        self.breakers = BreakerRepository(self.database)

    def invalidate_degraded_steps(self, run_id: str, item_key: str) -> list[str]:
        """Delete an item's checkpoints from its first degraded step onward and mark it running.

        Returns the names of the deleted steps, or an empty list when the item has no degraded step. ``WorkflowRecovery.heal()`` calls this before rerunning the item.
        """
        first_sequence = self.steps.first_degraded_sequence(run_id, item_key)
        if first_sequence is None:
            return []
        step_names = self.steps.names_from_sequence(run_id, item_key, first_sequence)
        self.steps.delete_from_sequence(run_id, item_key, first_sequence)
        self.items.reset_to_running(run_id, item_key)
        return step_names

    def delete_run(self, run_id: str) -> bool:
        """Delete everything stored for one run: its items, step checkpoints and audit records.

        ``run_id`` is ``"workflow_name:batch_label"``, for example ``"orders-sync:default"``. The next ``run_many`` with that name and label starts from scratch. Provider and breaker state is shared between runs and is kept. Returns ``False`` when the run does not exist.
        """
        with self.database.transaction():
            existed = self.runs.delete(run_id)
            self.items.delete_run(run_id)
            self.steps.delete_run(run_id)
            self.audit.delete_run(run_id)
        return existed

    def close(self) -> None:
        """Close the database connection."""
        self.database.close()

    def delete(self) -> None:
        """Close the store and delete its SQLite file, along with the WAL and shared-memory files next to it.

        Close any other ``CheckpointStore`` open on the same file first; on Windows an open file cannot be deleted. The store cannot be used afterwards; open a new ``CheckpointStore`` on the same path to start fresh.
        """
        self.database.delete()
