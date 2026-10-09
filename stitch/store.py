"""Durable state on top of sqlite3 (standard library).

One file holds everything the engine needs to survive a restart:

    runs, items, steps   the checkpoints themselves
    audit                append-only log of everything that happened, for tracing
    providers, breakers  the registry of fallback providers and their cooldowns

Every write is its own committed transaction, so after a SIGKILL the file holds
exactly what had happened before the kill and nothing else.

Step status:  running | done | degraded | waiting | failed
Item status:  running | done | parked | failed
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    workflow    TEXT NOT NULL,
    status      TEXT NOT NULL,          -- running | done
    params      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    run_id      TEXT NOT NULL,
    item        TEXT NOT NULL,
    status      TEXT NOT NULL,
    seq         INTEGER NOT NULL,       -- order in which items were first seen
    input       TEXT,                   -- the JSON the workflow was called with
    result      TEXT,                   -- the JSON the workflow returned
    degraded    INTEGER NOT NULL DEFAULT 0,
    retry_at    REAL,                   -- parked until (epoch seconds)
    error       TEXT,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (run_id, item)
);
CREATE TABLE IF NOT EXISTS steps (
    run_id      TEXT NOT NULL,
    item        TEXT NOT NULL,
    step        TEXT NOT NULL,
    seq         INTEGER NOT NULL,       -- position of the step within the item
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    result      TEXT,
    error       TEXT,
    started_at  REAL NOT NULL,
    finished_at REAL,
    PRIMARY KEY (run_id, item, step)
);
CREATE TABLE IF NOT EXISTS audit (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    run_id      TEXT,
    item        TEXT,
    step        TEXT,
    event       TEXT NOT NULL,
    attempt     INTEGER,
    provider    TEXT,
    duration    REAL,
    detail      TEXT
);
CREATE INDEX IF NOT EXISTS audit_item ON audit (item, id);
CREATE TABLE IF NOT EXISTS providers (
    pool        TEXT NOT NULL,
    name        TEXT NOT NULL,
    priority    INTEGER NOT NULL,       -- lower is tried first
    enabled     INTEGER NOT NULL DEFAULT 1,
    config      TEXT NOT NULL,
    registered_at REAL NOT NULL,
    PRIMARY KEY (pool, name)
);
CREATE TABLE IF NOT EXISTS breakers (
    pool        TEXT NOT NULL,
    name        TEXT NOT NULL,
    state       TEXT NOT NULL,          -- closed | open
    failures    INTEGER NOT NULL DEFAULT 0,
    opens       INTEGER NOT NULL DEFAULT 0,   -- consecutive opens, drives backoff
    retry_at    REAL,
    reason      TEXT,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (pool, name)
);
"""

_UNSET: Any = object()


@dataclass(frozen=True)
class StepRecord:
    item: str
    step: str
    seq: int
    status: str
    attempts: int
    result: Any
    error: str | None


@dataclass(frozen=True)
class AuditRow:
    id: int
    ts: float
    run_id: str | None
    item: str | None
    step: str | None
    event: str
    attempt: int | None
    provider: str | None
    duration: float | None
    detail: dict


@dataclass(frozen=True)
class ProviderRow:
    pool: str
    name: str
    priority: int
    enabled: bool
    config: dict


@dataclass(frozen=True)
class BreakerRow:
    pool: str
    name: str
    state: str
    failures: int
    opens: int
    retry_at: float | None
    reason: str | None


def dumps(value: Any) -> str | None:
    """JSON for storage, or None when the value can't be represented."""
    try:
        return json.dumps(value)
    except (TypeError, ValueError):
        return None


def loads(text: str | None) -> Any:
    return json.loads(text) if text is not None else None


class Store:
    """State in a single sqlite file. Use from one thread (the event loop)."""

    durable = True

    def __init__(self, path: str | Path):
        self.path = Path(path) if str(path) != ":memory:" else None
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), isolation_level=None)
        # WAL + synchronous=NORMAL survives a killed *process* (our demo).
        # Use synchronous=FULL if you also need to survive power loss.
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.execute("PRAGMA busy_timeout=2000")
        self._check_schema()

    def _check_schema(self) -> None:
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        has_tables = self._db.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'items'").fetchone()
        if has_tables and version != SCHEMA_VERSION:
            raise RuntimeError(
                f"{self.path} was written by an older version (schema {version}, "
                f"need {SCHEMA_VERSION}). Delete it, or run: python -m demo reset")
        self._db.executescript(SCHEMA)
        self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def close(self) -> None:
        self._db.close()

    # -- runs -------------------------------------------------------------

    def open_run(self, run_id: str, workflow: str, params: dict) -> str | None:
        """Create the run if it is new. Returns the previous status, or None."""
        row = self._db.execute(
            "SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        now = time.time()
        if row is None:
            self._db.execute(
                "INSERT INTO runs VALUES (?, ?, 'running', ?, ?, ?)",
                (run_id, workflow, json.dumps(params), now, now))
            return None
        self._db.execute(
            "UPDATE runs SET status = 'running', updated_at = ? WHERE run_id = ?",
            (now, run_id))
        return row[0]

    def finish_run(self, run_id: str) -> None:
        self._db.execute(
            "UPDATE runs SET status = 'done', updated_at = ? WHERE run_id = ?",
            (time.time(), run_id))

    def runs(self) -> list[tuple[str, str, str]]:
        return self._db.execute(
            "SELECT run_id, workflow, status FROM runs ORDER BY created_at").fetchall()

    # -- items ------------------------------------------------------------

    def item_row(self, run_id: str, item: str) -> dict | None:
        row = self._db.execute(
            """SELECT status, input, result, degraded, retry_at, error
               FROM items WHERE run_id = ? AND item = ?""", (run_id, item)).fetchone()
        if row is None:
            return None
        status, inp, result, degraded, retry_at, error = row
        return {"status": status, "input": loads(inp), "result": loads(result),
                "degraded": bool(degraded), "retry_at": retry_at, "error": error}

    def item_status(self, run_id: str, item: str) -> str | None:
        row = self._db.execute(
            "SELECT status FROM items WHERE run_id = ? AND item = ?", (run_id, item)).fetchone()
        return row[0] if row else None

    def set_item(self, run_id: str, item: str, status: str, *, input: Any = _UNSET,
                 result: Any = _UNSET, degraded: Any = _UNSET, retry_at: Any = _UNSET,
                 error: Any = _UNSET) -> None:
        """Create or update an item. `input` and `result` are JSON text already."""
        now = time.time()
        self._db.execute(
            """INSERT OR IGNORE INTO items (run_id, item, status, seq, updated_at)
               VALUES (?, ?, ?, (SELECT COUNT(*) FROM items WHERE run_id = ?), ?)""",
            (run_id, item, status, run_id, now))
        fields = {"status": status, "updated_at": now, "input": input, "result": result,
                  "degraded": degraded, "retry_at": retry_at, "error": error}
        fields = {k: v for k, v in fields.items() if v is not _UNSET}
        sets = ", ".join(f"{k} = ?" for k in fields)
        self._db.execute(f"UPDATE items SET {sets} WHERE run_id = ? AND item = ?",
                         (*fields.values(), run_id, item))

    def items(self, run_id: str) -> list[tuple[str, str]]:
        return self._db.execute(
            "SELECT item, status FROM items WHERE run_id = ? ORDER BY seq", (run_id,)).fetchall()

    def parked(self, run_id: str) -> list[tuple[str, Any, float]]:
        rows = self._db.execute(
            """SELECT item, input, retry_at FROM items
               WHERE run_id = ? AND status = 'parked' ORDER BY retry_at""", (run_id,)).fetchall()
        return [(item, loads(inp), retry_at) for item, inp, retry_at in rows]

    def resumable(self, run_id: str) -> list[tuple[str, Any]]:
        """Items that were interrupted or parked, with their stored inputs."""
        rows = self._db.execute(
            """SELECT item, input FROM items
               WHERE run_id = ? AND status IN ('running', 'parked') AND input IS NOT NULL
               ORDER BY seq""", (run_id,)).fetchall()
        return [(item, loads(inp)) for item, inp in rows]

    def degraded_items(self, run_id: str) -> list[tuple[str, Any]]:
        rows = self._db.execute(
            """SELECT item, input FROM items
               WHERE run_id = ? AND degraded = 1 AND input IS NOT NULL ORDER BY seq""",
            (run_id,)).fetchall()
        return [(item, loads(inp)) for item, inp in rows]

    def invalidate_degraded(self, run_id: str, item: str) -> list[str]:
        """Forget the first degraded step and everything after it, so a rerun redoes them."""
        row = self._db.execute(
            """SELECT MIN(seq) FROM steps
               WHERE run_id = ? AND item = ? AND status = 'degraded'""", (run_id, item)).fetchone()
        if row is None or row[0] is None:
            return []
        names = [r[0] for r in self._db.execute(
            "SELECT step FROM steps WHERE run_id = ? AND item = ? AND seq >= ? ORDER BY seq",
            (run_id, item, row[0]))]
        self._db.execute("DELETE FROM steps WHERE run_id = ? AND item = ? AND seq >= ?",
                         (run_id, item, row[0]))
        # keep degraded=1: it is cleared only when the rerun succeeds, so a failed heal can be retried
        self.set_item(run_id, item, "running")
        return names

    # -- steps ------------------------------------------------------------

    def get_step(self, run_id: str, item: str, step: str) -> StepRecord | None:
        row = self._db.execute(
            """SELECT item, step, seq, status, attempts, result, error
               FROM steps WHERE run_id = ? AND item = ? AND step = ?""",
            (run_id, item, step)).fetchone()
        return _record(row) if row else None

    def step_started(self, run_id: str, item: str, step: str, seq: int) -> None:
        self._db.execute(
            """INSERT INTO steps (run_id, item, step, seq, status, started_at)
               VALUES (?, ?, ?, ?, 'running', ?)
               ON CONFLICT (run_id, item, step)
               DO UPDATE SET status = 'running', started_at = excluded.started_at""",
            (run_id, item, step, seq, time.time()))

    def step_done(self, run_id: str, item: str, step: str, result: Any, attempts: int,
                  degraded: bool = False) -> None:
        self._db.execute(
            """UPDATE steps SET status = ?, result = ?, attempts = ?,
                   error = NULL, finished_at = ?
               WHERE run_id = ? AND item = ? AND step = ?""",
            ("degraded" if degraded else "done", json.dumps(result), attempts,
             time.time(), run_id, item, step))

    def step_failed(self, run_id: str, item: str, step: str, error: str, attempts: int) -> None:
        self._update_step(run_id, item, step, "failed", error, attempts)

    def step_waiting(self, run_id: str, item: str, step: str, error: str, attempts: int) -> None:
        self._update_step(run_id, item, step, "waiting", error, attempts)

    def _update_step(self, run_id, item, step, status, error, attempts) -> None:
        self._db.execute(
            """UPDATE steps SET status = ?, error = ?, attempts = ?, finished_at = ?
               WHERE run_id = ? AND item = ? AND step = ?""",
            (status, error, attempts, time.time(), run_id, item, step))

    def steps(self, run_id: str) -> list[StepRecord]:
        rows = self._db.execute(
            """SELECT s.item, s.step, s.seq, s.status, s.attempts, s.result, s.error
               FROM steps s JOIN items i ON i.run_id = s.run_id AND i.item = s.item
               WHERE s.run_id = ? ORDER BY i.seq, s.seq""", (run_id,)).fetchall()
        return [_record(r) for r in rows]

    # -- audit ------------------------------------------------------------

    def audit(self, run_id: str | None, event: str, data: dict) -> None:
        data = dict(data)
        item, step = data.pop("item", None), data.pop("step", None)
        attempt = data.pop("attempt", None)
        provider = data.pop("provider", None)
        duration = data.pop("elapsed", None)
        data.pop("result", None)  # results live in the checkpoint, not the audit trail
        self._db.execute(
            """INSERT INTO audit (ts, run_id, item, step, event, attempt, provider, duration, detail)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (time.time(), run_id, item, step, event, attempt, provider, duration,
             json.dumps(data, default=_brief) if data else None))

    def audit_rows(self, *, item: str | None = None, after_id: int = 0,
                   limit: int | None = None) -> list[AuditRow]:
        where, args = ["id > ?"], [after_id]
        if item is not None:
            # Provider events carry the item too; run-level events (no item) are included
            # only when no item filter is given.
            where.append("item = ?")
            args.append(item)
        sql = f"""SELECT id, ts, run_id, item, step, event, attempt, provider, duration, detail
                  FROM audit WHERE {' AND '.join(where)} ORDER BY id"""
        if limit is not None:
            sql = f"SELECT * FROM ({sql.replace('ORDER BY id', 'ORDER BY id DESC')} LIMIT {int(limit)}) ORDER BY id"
        return [AuditRow(*r[:9], loads(r[9]) or {}) for r in self._db.execute(sql, args)]

    # -- provider registry and breakers -------------------------------------

    def list_providers(self, pool: str) -> list[ProviderRow]:
        rows = self._db.execute(
            """SELECT pool, name, priority, enabled, config FROM providers
               WHERE pool = ? ORDER BY priority, name""", (pool,)).fetchall()
        return [ProviderRow(p, n, pr, bool(en), json.loads(c)) for p, n, pr, en, c in rows]

    def put_provider(self, pool: str, name: str, priority: int, config: dict,
                     enabled: bool = True) -> None:
        self._db.execute(
            """INSERT INTO providers VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT (pool, name) DO UPDATE SET
                   priority = excluded.priority, enabled = excluded.enabled,
                   config = excluded.config""",
            (pool, name, priority, int(enabled), json.dumps(config), time.time()))

    def set_provider_enabled(self, pool: str, name: str, enabled: bool) -> bool:
        cur = self._db.execute("UPDATE providers SET enabled = ? WHERE pool = ? AND name = ?",
                               (int(enabled), pool, name))
        return cur.rowcount > 0

    def delete_provider(self, pool: str, name: str) -> bool:
        cur = self._db.execute("DELETE FROM providers WHERE pool = ? AND name = ?", (pool, name))
        self._db.execute("DELETE FROM breakers WHERE pool = ? AND name = ?", (pool, name))
        return cur.rowcount > 0

    def get_breaker(self, pool: str, name: str) -> BreakerRow | None:
        row = self._db.execute(
            """SELECT pool, name, state, failures, opens, retry_at, reason
               FROM breakers WHERE pool = ? AND name = ?""", (pool, name)).fetchone()
        return BreakerRow(*row) if row else None

    def put_breaker(self, pool: str, name: str, state: str, failures: int, opens: int,
                    retry_at: float | None, reason: str | None) -> None:
        self._db.execute(
            """INSERT INTO breakers VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (pool, name) DO UPDATE SET
                   state = excluded.state, failures = excluded.failures, opens = excluded.opens,
                   retry_at = excluded.retry_at, reason = excluded.reason,
                   updated_at = excluded.updated_at""",
            (pool, name, state, failures, opens, retry_at, reason, time.time()))

    def breakers(self, pool: str) -> dict[str, BreakerRow]:
        rows = self._db.execute(
            """SELECT pool, name, state, failures, opens, retry_at, reason
               FROM breakers WHERE pool = ?""", (pool,)).fetchall()
        return {r[1]: BreakerRow(*r) for r in rows}

    def pools(self) -> list[str]:
        return [r[0] for r in self._db.execute("SELECT DISTINCT pool FROM providers ORDER BY pool")]


class NullStore(Store):
    """Remembers nothing about steps: what a plain task queue gives each job.

    The provider registry and breakers still work (in memory) so a Pool behaves
    the same way in both modes.
    """

    durable = False

    def __init__(self):
        super().__init__(":memory:")

    def open_run(self, run_id, workflow, params): return None
    def finish_run(self, run_id): ...
    def item_row(self, run_id, item): return None
    def item_status(self, run_id, item): return None
    def set_item(self, run_id, item, status, **fields): ...
    def parked(self, run_id): return []
    def get_step(self, run_id, item, step): return None
    def step_started(self, run_id, item, step, seq): ...
    def step_done(self, run_id, item, step, result, attempts, degraded=False): ...
    def step_failed(self, run_id, item, step, error, attempts): ...
    def step_waiting(self, run_id, item, step, error, attempts): ...
    def audit(self, run_id, event, data): ...


def _record(row) -> StepRecord:
    item, step, seq, status, attempts, result, error = row
    return StepRecord(item, step, seq, status, attempts, loads(result), error)


def _brief(value: Any) -> str:
    text = str(value)
    return text if len(text) <= 200 else text[:197] + "..."
