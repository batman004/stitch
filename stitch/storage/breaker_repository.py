import time

from ..records import BreakerRecord
from .database import Database


class BreakerRepository:
    def __init__(self, database: Database):
        self.database = database

    def get(self, pool_name: str, name: str) -> BreakerRecord | None:
        row = self.database.fetch_one(
            "SELECT pool_name, name, state, failures, consecutive_opens, retry_at, reason "
            "FROM breakers WHERE pool_name = ? AND name = ?",
            (pool_name, name),
        )
        return BreakerRecord(*row) if row else None

    def list_breakers(self, pool_name: str) -> dict[str, BreakerRecord]:
        rows = self.database.fetch_all(
            "SELECT pool_name, name, state, failures, consecutive_opens, retry_at, reason FROM breakers WHERE pool_name = ?",
            (pool_name,),
        )
        return {row[1]: BreakerRecord(*row) for row in rows}

    def save(self, record: BreakerRecord) -> None:
        self.database.execute(
            "INSERT INTO breakers VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (pool_name, name) DO UPDATE SET state = excluded.state, failures = excluded.failures, "
            "consecutive_opens = excluded.consecutive_opens, retry_at = excluded.retry_at, "
            "reason = excluded.reason, updated_at = excluded.updated_at",
            (
                record.pool_name, record.name, record.state, record.failures,
                record.consecutive_opens, record.retry_at, record.reason, time.time(),
            ),
        )
