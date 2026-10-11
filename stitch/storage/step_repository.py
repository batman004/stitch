import json
import time
from typing import Any

from ..records import StepRecord
from ..serialization import from_json_text
from .database import Database


class StepRepository:
    def __init__(self, database: Database):
        self.database = database

    def get(self, run_id: str, item_key: str, step_name: str) -> StepRecord | None:
        row = self.database.fetch_one(
            "SELECT item_key, step_name, sequence, status, attempts, result, error FROM steps "
            "WHERE run_id = ? AND item_key = ? AND step_name = ?",
            (run_id, item_key, step_name),
        )
        return self._to_record(row) if row else None

    def list_for_run(self, run_id: str) -> list[StepRecord]:
        rows = self.database.fetch_all(
            "SELECT s.item_key, s.step_name, s.sequence, s.status, s.attempts, s.result, s.error "
            "FROM steps s JOIN items i ON i.run_id = s.run_id AND i.item_key = s.item_key "
            "WHERE s.run_id = ? ORDER BY i.sequence, s.sequence",
            (run_id,),
        )
        return [self._to_record(row) for row in rows]

    def mark_started(self, run_id: str, item_key: str, step_name: str, sequence: int) -> None:
        self.database.execute(
            "INSERT INTO steps (run_id, item_key, step_name, sequence, status, started_at) "
            "VALUES (?, ?, ?, ?, 'running', ?) "
            "ON CONFLICT (run_id, item_key, step_name) "
            "DO UPDATE SET status = 'running', started_at = excluded.started_at",
            (run_id, item_key, step_name, sequence, time.time()),
        )

    def mark_done(self, run_id: str, item_key: str, step_name: str, result: Any, attempts: int, degraded: bool) -> None:
        status = "degraded" if degraded else "done"
        self.database.execute(
            "UPDATE steps SET status = ?, result = ?, attempts = ?, error = NULL, finished_at = ? "
            "WHERE run_id = ? AND item_key = ? AND step_name = ?",
            (status, json.dumps(result), attempts, time.time(), run_id, item_key, step_name),
        )

    def mark_failed(self, run_id: str, item_key: str, step_name: str, error: str, attempts: int) -> None:
        self._mark_unfinished(run_id, item_key, step_name, "failed", error, attempts)

    def mark_waiting(self, run_id: str, item_key: str, step_name: str, error: str, attempts: int) -> None:
        self._mark_unfinished(run_id, item_key, step_name, "waiting", error, attempts)

    def first_degraded_sequence(self, run_id: str, item_key: str) -> int | None:
        row = self.database.fetch_one(
            "SELECT MIN(sequence) FROM steps WHERE run_id = ? AND item_key = ? AND status = 'degraded'",
            (run_id, item_key),
        )
        return row[0]

    def names_from_sequence(self, run_id: str, item_key: str, sequence: int) -> list[str]:
        rows = self.database.fetch_all(
            "SELECT step_name FROM steps WHERE run_id = ? AND item_key = ? AND sequence >= ? ORDER BY sequence",
            (run_id, item_key, sequence),
        )
        return [row[0] for row in rows]

    def delete_from_sequence(self, run_id: str, item_key: str, sequence: int) -> None:
        self.database.execute(
            "DELETE FROM steps WHERE run_id = ? AND item_key = ? AND sequence >= ?",
            (run_id, item_key, sequence),
        )

    def _mark_unfinished(self, run_id: str, item_key: str, step_name: str, status: str, error: str, attempts: int) -> None:
        self.database.execute(
            "UPDATE steps SET status = ?, error = ?, attempts = ?, finished_at = ? "
            "WHERE run_id = ? AND item_key = ? AND step_name = ?",
            (status, error, attempts, time.time(), run_id, item_key, step_name),
        )

    def _to_record(self, row: tuple) -> StepRecord:
        item_key, step_name, sequence, status, attempts, result_text, error = row
        return StepRecord(item_key, step_name, sequence, status, attempts, from_json_text(result_text), error)
