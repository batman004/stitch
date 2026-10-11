from typing import Any

from ..storage.checkpoint_store import CheckpointStore
from .reporting import Reporter, RunStatistics


class RunSession:
    def __init__(self, run_id: str, store: CheckpointStore, reporter: Reporter, record_audit: bool = True):
        self.run_id = run_id
        self.store = store
        self.reporter = reporter
        self.record_audit = record_audit
        self.statistics = RunStatistics()

    def emit(self, event: str, **data: Any) -> None:
        if event == "failover":
            self.statistics.failovers += 1
        if self.record_audit:
            self.store.audit.record(self.run_id, event, data)
        self.reporter.emit(event, **data)
