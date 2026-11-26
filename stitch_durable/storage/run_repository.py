import json
import time

from .database import Database


class RunRepository:
    def __init__(self, database: Database):
        self.database = database

    def open_run(self, run_id: str, workflow_name: str, parameters: dict) -> str | None:
        existing = self.database.fetch_one(
            "SELECT status FROM runs WHERE run_id = ?", (run_id,)
        )
        now = time.time()
        if existing is None:
            self.database.execute(
                "INSERT INTO runs (run_id, workflow, status, parameters, created_at, updated_at) "
                "VALUES (?, ?, 'running', ?, ?, ?)",
                (run_id, workflow_name, json.dumps(parameters), now, now),
            )
            return None
        self.database.execute(
            "UPDATE runs SET status = 'running', halted_step = NULL, halt_reason = NULL, updated_at = ? WHERE run_id = ?",
            (now, run_id),
        )
        return existing[0]

    def ensure_pending(self, run_id: str, workflow_name: str) -> None:
        """Make sure the run exists so recovery can find it, and mark a finished run as having new work."""
        now = time.time()
        self.database.execute(
            "INSERT OR IGNORE INTO runs (run_id, workflow, status, parameters, created_at, updated_at) "
            "VALUES (?, ?, 'pending', '{}', ?, ?)",
            (run_id, workflow_name, now, now),
        )
        self.database.execute(
            "UPDATE runs SET status = 'pending', updated_at = ? WHERE run_id = ? AND status = 'done'",
            (now, run_id),
        )

    def finish_run(self, run_id: str) -> None:
        self.database.execute(
            "UPDATE runs SET status = 'done', updated_at = ? WHERE run_id = ?",
            (time.time(), run_id),
        )

    def mark_halted(self, run_id: str, step_name: str, reason: str) -> None:
        self.database.execute(
            "UPDATE runs SET status = 'halted', halted_step = ?, halt_reason = ?, updated_at = ? WHERE run_id = ?",
            (step_name, reason, time.time(), run_id),
        )

    def get_halt(self, run_id: str) -> tuple[str, str] | None:
        return self.database.fetch_one(
            "SELECT halted_step, halt_reason FROM runs WHERE run_id = ? AND status = 'halted'",
            (run_id,),
        )

    def delete(self, run_id: str) -> bool:
        cursor = self.database.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        return cursor.rowcount > 0

    def list_runs(self) -> list[tuple[str, str, str]]:
        return self.database.fetch_all(
            "SELECT run_id, workflow, status FROM runs ORDER BY created_at"
        )
