import time

from ..records import ItemInput, ItemRecord, ParkedItem
from ..serialization import from_json_text
from .database import Database


class ItemRepository:
    def __init__(self, database: Database):
        self.database = database

    def get(self, run_id: str, item_key: str) -> ItemRecord | None:
        row = self.database.fetch_one(
            "SELECT status, input, result, degraded, retry_at, error FROM items WHERE run_id = ? AND item_key = ?",
            (run_id, item_key),
        )
        if row is None:
            return None
        status, input_text, result_text, degraded, retry_at, error = row
        return ItemRecord(status, from_json_text(input_text), from_json_text(result_text), bool(degraded), retry_at, error)

    def list_statuses(self, run_id: str) -> list[tuple[str, str]]:
        return self.database.fetch_all(
            "SELECT item_key, status FROM items WHERE run_id = ? ORDER BY sequence", (run_id,)
        )

    def mark_running(self, run_id: str, item_key: str, input_text: str | None) -> None:
        self._ensure_row(run_id, item_key)
        self._update(run_id, item_key, status="running", input=input_text, retry_at=None, error=None)

    def mark_done(self, run_id: str, item_key: str, result_text: str | None, degraded: bool) -> None:
        self._update(run_id, item_key, status="done", result=result_text, degraded=int(degraded), retry_at=None, error=None)

    def mark_parked(self, run_id: str, item_key: str, retry_at: float, reason: str) -> None:
        self._update(run_id, item_key, status="parked", retry_at=retry_at, error=reason)

    def mark_failed(self, run_id: str, item_key: str, error: str) -> None:
        self._update(run_id, item_key, status="failed", error=error)

    def reset_to_running(self, run_id: str, item_key: str) -> None:
        self._update(run_id, item_key, status="running")

    def list_parked(self, run_id: str) -> list[ParkedItem]:
        rows = self.database.fetch_all(
            "SELECT item_key, input, retry_at FROM items WHERE run_id = ? AND status = 'parked' ORDER BY retry_at",
            (run_id,),
        )
        return [ParkedItem(item_key, from_json_text(input_text), retry_at) for item_key, input_text, retry_at in rows]

    def list_resumable(self, run_id: str) -> list[ItemInput]:
        return self._list_inputs(
            "SELECT item_key, input FROM items WHERE run_id = ? AND status IN ('running', 'parked') "
            "AND input IS NOT NULL ORDER BY sequence",
            run_id,
        )

    def list_degraded(self, run_id: str) -> list[ItemInput]:
        return self._list_inputs(
            "SELECT item_key, input FROM items WHERE run_id = ? AND degraded = 1 "
            "AND input IS NOT NULL ORDER BY sequence",
            run_id,
        )

    def _list_inputs(self, sql: str, run_id: str) -> list[ItemInput]:
        rows = self.database.fetch_all(sql, (run_id,))
        return [ItemInput(item_key, from_json_text(input_text)) for item_key, input_text in rows]

    def _ensure_row(self, run_id: str, item_key: str) -> None:
        self.database.execute(
            "INSERT OR IGNORE INTO items (run_id, item_key, status, sequence, updated_at) "
            "VALUES (?, ?, 'running', (SELECT COUNT(*) FROM items WHERE run_id = ?), ?)",
            (run_id, item_key, run_id, time.time()),
        )

    def _update(self, run_id: str, item_key: str, **columns) -> None:
        columns["updated_at"] = time.time()
        assignments = ", ".join(f"{column} = ?" for column in columns)
        self.database.execute(
            f"UPDATE items SET {assignments} WHERE run_id = ? AND item_key = ?",
            (*columns.values(), run_id, item_key),
        )
