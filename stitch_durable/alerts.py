from dataclasses import dataclass


@dataclass(frozen=True)
class Alert:
    """A message for an ``Alerter`` about a critical step.

    ``severity`` is ``"critical"`` when the run halts and ``"recovered"`` when it continues. ``title`` is a one-line summary. ``detail`` holds extra fields such as the failing ``item`` and ``error``.
    """

    severity: str
    title: str
    run_id: str
    step_name: str
    detail: dict


class Alerter:
    """Subclass and override ``send`` to deliver alerts, for example to Slack or a pager.

    Pass an instance to ``run_many(..., alerter=...)``. The base class ignores every alert.

    Example::

        class PrintAlerter(Alerter):
            def send(self, alert):
                print(f"{alert.severity}: {alert.title}")
    """

    def send(self, alert: Alert) -> None:
        """Deliver ``alert``. An exception here is recorded as ``alert_failed`` and does not stop the run."""
        return
