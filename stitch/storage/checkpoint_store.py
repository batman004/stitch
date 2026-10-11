from pathlib import Path

from .audit_repository import AuditRepository
from .breaker_repository import BreakerRepository
from .database import Database
from .item_repository import ItemRepository
from .provider_repository import ProviderRepository
from .run_repository import RunRepository
from .step_repository import StepRepository


class CheckpointStore:
    def __init__(self, path: str | Path):
        self.database = Database(path)
        self.runs = RunRepository(self.database)
        self.items = ItemRepository(self.database)
        self.steps = StepRepository(self.database)
        self.audit = AuditRepository(self.database)
        self.providers = ProviderRepository(self.database)
        self.breakers = BreakerRepository(self.database)

    def invalidate_degraded_steps(self, run_id: str, item_key: str) -> list[str]:
        first_sequence = self.steps.first_degraded_sequence(run_id, item_key)
        if first_sequence is None:
            return []
        step_names = self.steps.names_from_sequence(run_id, item_key, first_sequence)
        self.steps.delete_from_sequence(run_id, item_key, first_sequence)
        self.items.reset_to_running(run_id, item_key)
        return step_names

    def close(self) -> None:
        self.database.close()
