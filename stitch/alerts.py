from dataclasses import dataclass


@dataclass(frozen=True)
class Alert:
    severity: str
    title: str
    run_id: str
    step_name: str
    detail: dict


class Alerter:
    def send(self, alert: Alert) -> None:
        return
