import json
import time

from ..records import AuditRecord
from ..serialization import from_json_text
from .database import Database

MAXIMUM_DETAIL_TEXT_LENGTH = 200
EVENT_FIELDS = ("item", "step", "attempt", "provider", "elapsed")


class AuditRepository:
    def __init__(self, database: Database):
        self.database = database

    def record(self, run_id: str | None, event: str, data: dict) -> None:
        detail = dict(data)
        fields = {name: detail.pop(name, None) for name in EVENT_FIELDS}
        detail.pop("result", None)
        self.database.execute(
            "INSERT INTO audit (timestamp, run_id, item_key, step_name, event, attempt, provider, duration_seconds, detail) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                time.time(), run_id, fields["item"], fields["step"], event,
                fields["attempt"], fields["provider"], fields["elapsed"], self._encode_detail(detail),
            ),
        )

    def list_records(self, item_key: str | None = None, after_record_id: int = 0) -> list[AuditRecord]:
        sql = (
            "SELECT record_id, timestamp, run_id, item_key, step_name, event, attempt, provider, duration_seconds, detail "
            "FROM audit WHERE record_id > ?"
        )
        parameters: tuple = (after_record_id,)
        if item_key is not None:
            sql += " AND item_key = ?"
            parameters = (after_record_id, item_key)
        rows = self.database.fetch_all(sql + " ORDER BY record_id", parameters)
        return [AuditRecord(*row[:9], from_json_text(row[9]) or {}) for row in rows]

    def find_item_keys(self, prefix: str) -> list[str]:
        rows = self.database.fetch_all(
            "SELECT DISTINCT item_key FROM audit WHERE item_key LIKE ?", (prefix + "%",)
        )
        return [row[0] for row in rows]

    def _encode_detail(self, detail: dict) -> str | None:
        if not detail:
            return None
        return json.dumps(detail, default=self._shorten)

    def _shorten(self, value) -> str:
        text = str(value)
        if len(text) <= MAXIMUM_DETAIL_TEXT_LENGTH:
            return text
        return text[: MAXIMUM_DETAIL_TEXT_LENGTH - 3] + "..."
