from typing import Any

from ..alerts import Alert, Alerter
from ..errors import CriticalStepFailed, describe_error
from ..storage.checkpoint_store import CheckpointStore
from .reporting import Reporter, RunStatistics


class RunSession:
    def __init__(self, run_id: str, store: CheckpointStore, reporter: Reporter, record_audit: bool = True, alerter: Alerter | None = None):
        self.run_id = run_id
        self.store = store
        self.reporter = reporter
        self.record_audit = record_audit
        self.statistics = RunStatistics()
        self.alerter = alerter or Alerter()
        self.critical_failure: CriticalStepFailed | None = None

    def emit(self, event: str, **data: Any) -> None:
        if event == "failover":
            self.statistics.failovers += 1
        if self.record_audit:
            self.store.audit.record(self.run_id, event, data)
        self.reporter.emit(event, **data)

    def register_critical_failure(self, failure: CriticalStepFailed) -> CriticalStepFailed:
        if self.critical_failure is not None:
            return self.critical_failure
        self.critical_failure = failure
        self.emit("critical_failure", item=failure.item_key, step=failure.step_name, error=describe_error(failure.cause))
        detail = {"item": failure.item_key, "error": describe_error(failure.cause), "items_done": self.statistics.items_done}
        self.send_alert(Alert("critical", f"{failure.step_name} is down, the run is halted", self.run_id, failure.step_name, detail))
        return failure

    def send_alert(self, alert: Alert) -> None:
        try:
            self.alerter.send(alert)
        except Exception as error:
            self.emit("alert_failed", step=alert.step_name, error=describe_error(error))
            return
        self.emit("alert_sent", step=alert.step_name, severity=alert.severity)
