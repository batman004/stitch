"""Render the audit log as a readable timeline.

    print(render(store.audit_rows(item="evt-0007")))

Every state change the engine makes is in the audit table: which step started,
which attempt failed and why, which provider answered, which breaker opened,
when an item was parked and when it came back. This turns a row dump into the
story of one item.
"""

from __future__ import annotations

import time

from .store import AuditRow

# event -> (marker, colour code). Unlisted events print as a dim dot.
STYLE = {
    "item_started": ("●", "1"), "item_done": ("■", "32"), "item_skipped": ("↷", "2"),
    "item_failed": ("✗", "31"), "item_parked": ("⏸", "33"), "item_retry": ("⟲", "31"),
    "step_started": ("▸", "0"), "step_done": ("✓", "32"), "step_skipped": ("↷", "2"),
    "step_failed": ("✗", "31"), "step_retry": ("⟳", "33"), "step_degraded": ("↓", "35"),
    "step_waiting": ("⏸", "33"),
    "provider_used": ("→", "36"), "provider_failed": ("!", "33"), "failover": ("↪", "36"),
    "breaker_opened": ("⛔", "31"), "breaker_closed": ("✔", "32"), "breaker_probe": ("?", "33"),
    "heal_started": ("♻", "35"), "provider_registered": ("＋", "36"), "crash": ("💥", "1;31"),
    "cooldown_wait": ("⏳", "33"), "cooldown_over": ("⏰", "32"),
}


def _sentence(r: AuditRow) -> str:
    d = r.detail
    e = r.event
    who = f" via {r.provider}" if r.provider else ""
    if e == "step_started":
        return f"{r.step} started" + ("  (was interrupted last time)" if d.get("interrupted") else "")
    if e == "step_done":
        extra = " DEGRADED" if d.get("degraded") else ""
        tries = f" after {r.attempt} attempts" if r.attempt and r.attempt > 1 else ""
        return f"{r.step} done in {r.duration:.2f}s{tries}{extra}" if r.duration is not None else f"{r.step} done"
    if e == "step_skipped":
        return f"{r.step} skipped: checkpoint found" + (" (degraded)" if d.get("degraded") else "")
    if e == "step_retry":
        return f"{r.step} attempt {r.attempt} failed: {d.get('error')}; retrying in {float(d.get('delay', 0)):.1f}s"
    if e == "step_failed":
        return f"{r.step} FAILED: {d.get('error')}"
    if e == "step_degraded":
        return f"{r.step} used its fallback: {d.get('reason')}"
    if e == "step_waiting":
        return f"{r.step} waiting: {d.get('reason')}"
    if e == "provider_used":
        return f"{r.step}: answered by {r.provider}"
    if e == "provider_failed":
        return f"{r.step}: {r.provider} failed ({d.get('kind')}): {d.get('error')}"
    if e == "failover":
        return f"{r.step}: failed over {d.get('from_provider')} → {r.provider} ({d.get('cause')})"
    if e == "breaker_opened":
        return f"breaker OPEN on {r.provider} for {float(d.get('cooldown', 0)):g}s: {d.get('reason')}"
    if e == "breaker_closed":
        return f"breaker closed on {r.provider}: it is healthy again"
    if e == "breaker_probe":
        return f"probing {r.provider} after its cooldown"
    if e == "item_started":
        was = d.get("was")
        return "started" if was is None else f"started again (was {was})"
    if e == "item_parked":
        return f"PARKED until {time.strftime('%H:%M:%S', time.localtime(float(d.get('until', 0))))}: {d.get('reason')}"
    if e == "item_done":
        return "finished" + (" (degraded: fallback results published)" if d.get("degraded") else "")
    if e == "item_failed":
        return f"FAILED: {d.get('error')}"
    if e == "heal_started":
        return f"heal: redoing {d.get('redo')}"
    if e == "crash":
        return f"process killed in {r.step} ({d.get('phase')})"
    return e.replace("_", " ") + who


def render(rows: list[AuditRow], *, color: bool = False, hide: frozenset[str] = frozenset()) -> str:
    if not rows:
        return "no audit rows"
    t0 = rows[0].ts
    lines = []
    for r in rows:
        if r.event in hide:
            continue
        marker, code = STYLE.get(r.event, ("·", "2"))
        offset = f"+{r.ts - t0:7.2f}s"
        stamp = time.strftime("%H:%M:%S", time.localtime(r.ts)) + f".{int(r.ts % 1 * 1000):03d}"
        text = _sentence(r)
        indent = "  " if r.event.startswith(("step_", "provider_", "failover", "breaker_")) else ""
        line = f"{stamp} {offset}  {indent}{marker} {text}"
        lines.append(f"\033[{code}m{line}\033[0m" if color else line)
    return "\n".join(lines)
