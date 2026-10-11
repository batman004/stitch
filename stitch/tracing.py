import time

from .records import AuditRecord

EVENT_STYLES = {
    "item_started": ("●", "1"), "item_done": ("■", "32"), "item_skipped": ("↷", "2"),
    "item_failed": ("✗", "31"), "item_parked": ("⏸", "33"), "item_retry": ("⟲", "31"),
    "step_started": ("▸", "0"), "step_done": ("✓", "32"), "step_skipped": ("↷", "2"),
    "step_failed": ("✗", "31"), "step_retry": ("⟳", "33"), "step_degraded": ("↓", "35"),
    "step_waiting": ("⏸", "33"), "provider_used": ("→", "36"), "provider_failed": ("!", "33"),
    "failover": ("↪", "36"), "breaker_opened": ("⛔", "31"), "breaker_closed": ("✔", "32"),
    "breaker_probe": ("?", "33"), "heal_started": ("♻", "35"), "provider_registered": ("＋", "36"),
    "cooldown_wait": ("⏳", "33"), "cooldown_over": ("⏰", "32"),
}
INDENTED_PREFIXES = ("step_", "provider_", "failover", "breaker_")
DEFAULT_STYLE = ("·", "2")


class TraceRenderer:
    def __init__(self, use_color: bool = False):
        self.use_color = use_color

    def render(self, records: list[AuditRecord]) -> str:
        if not records:
            return "no audit records"
        first_timestamp = records[0].timestamp
        return "\n".join(self._render_line(record, first_timestamp) for record in records)

    def _render_line(self, record: AuditRecord, first_timestamp: float) -> str:
        marker, color_code = EVENT_STYLES.get(record.event, DEFAULT_STYLE)
        indent = "  " if record.event.startswith(INDENTED_PREFIXES) else ""
        offset = f"+{record.timestamp - first_timestamp:7.2f}s"
        line = f"{self._format_clock(record.timestamp)} {offset}  {indent}{marker} {self._describe(record)}"
        if self.use_color:
            return f"\033[{color_code}m{line}\033[0m"
        return line

    def _format_clock(self, timestamp: float) -> str:
        milliseconds = int(timestamp % 1 * 1000)
        return time.strftime("%H:%M:%S", time.localtime(timestamp)) + f".{milliseconds:03d}"

    def _describe(self, record: AuditRecord) -> str:
        describer = getattr(self, f"_describe_{record.event}", None)
        if describer is None:
            return record.event.replace("_", " ")
        return describer(record)

    def _describe_step_started(self, record: AuditRecord) -> str:
        suffix = "  (was interrupted last time)" if record.detail.get("interrupted") else ""
        return f"{record.step_name} started{suffix}"

    def _describe_step_done(self, record: AuditRecord) -> str:
        if record.duration_seconds is None:
            return f"{record.step_name} done"
        retries = f" after {record.attempt} attempts" if record.attempt and record.attempt > 1 else ""
        degraded = " DEGRADED" if record.detail.get("degraded") else ""
        return f"{record.step_name} done in {record.duration_seconds:.2f}s{retries}{degraded}"

    def _describe_step_skipped(self, record: AuditRecord) -> str:
        suffix = " (degraded)" if record.detail.get("degraded") else ""
        return f"{record.step_name} skipped: checkpoint found{suffix}"

    def _describe_step_retry(self, record: AuditRecord) -> str:
        delay = float(record.detail.get("delay", 0))
        return f"{record.step_name} attempt {record.attempt} failed: {record.detail.get('error')}; retrying in {delay:.1f}s"

    def _describe_step_failed(self, record: AuditRecord) -> str:
        return f"{record.step_name} FAILED: {record.detail.get('error')}"

    def _describe_step_degraded(self, record: AuditRecord) -> str:
        return f"{record.step_name} used its fallback: {record.detail.get('reason')}"

    def _describe_step_waiting(self, record: AuditRecord) -> str:
        return f"{record.step_name} waiting: {record.detail.get('reason')}"

    def _describe_provider_used(self, record: AuditRecord) -> str:
        return f"{record.step_name}: answered by {record.provider}"

    def _describe_provider_failed(self, record: AuditRecord) -> str:
        return f"{record.step_name}: {record.provider} failed ({record.detail.get('kind')}): {record.detail.get('error')}"

    def _describe_failover(self, record: AuditRecord) -> str:
        source = record.detail.get("from_provider")
        return f"{record.step_name}: failed over {source} → {record.provider} ({record.detail.get('cause')})"

    def _describe_breaker_opened(self, record: AuditRecord) -> str:
        cooldown = float(record.detail.get("cooldown", 0))
        return f"breaker OPEN on {record.provider} for {cooldown:g}s: {record.detail.get('reason')}"

    def _describe_breaker_closed(self, record: AuditRecord) -> str:
        return f"breaker closed on {record.provider}: it is healthy again"

    def _describe_breaker_probe(self, record: AuditRecord) -> str:
        return f"probing {record.provider} after its cooldown"

    def _describe_item_started(self, record: AuditRecord) -> str:
        previous = record.detail.get("was")
        return "started" if previous is None else f"started again (was {previous})"

    def _describe_item_parked(self, record: AuditRecord) -> str:
        until = time.strftime("%H:%M:%S", time.localtime(float(record.detail.get("until", 0))))
        return f"PARKED until {until}: {record.detail.get('reason')}"

    def _describe_item_done(self, record: AuditRecord) -> str:
        if record.detail.get("degraded"):
            return "finished (degraded: fallback results published)"
        return "finished"

    def _describe_item_failed(self, record: AuditRecord) -> str:
        return f"FAILED: {record.detail.get('error')}"

    def _describe_heal_started(self, record: AuditRecord) -> str:
        return f"heal: redoing {record.detail.get('redo')}"
