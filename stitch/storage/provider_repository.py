import json
import time

from ..records import ProviderRecord
from .database import Database


class ProviderRepository:
    def __init__(self, database: Database):
        self.database = database

    def list_providers(self, pool_name: str) -> list[ProviderRecord]:
        rows = self.database.fetch_all(
            "SELECT pool_name, name, priority, enabled, config FROM providers WHERE pool_name = ? ORDER BY priority, name",
            (pool_name,),
        )
        return [ProviderRecord(pool, name, priority, bool(enabled), json.loads(config)) for pool, name, priority, enabled, config in rows]

    def save(self, pool_name: str, name: str, priority: int, config: dict) -> None:
        self.database.execute(
            "INSERT INTO providers VALUES (?, ?, ?, 1, ?, ?) "
            "ON CONFLICT (pool_name, name) DO UPDATE SET priority = excluded.priority, config = excluded.config",
            (pool_name, name, priority, json.dumps(config), time.time()),
        )

    def set_enabled(self, pool_name: str, name: str, enabled: bool) -> bool:
        cursor = self.database.execute(
            "UPDATE providers SET enabled = ? WHERE pool_name = ? AND name = ?", (int(enabled), pool_name, name)
        )
        return cursor.rowcount > 0

    def delete(self, pool_name: str, name: str) -> bool:
        cursor = self.database.execute("DELETE FROM providers WHERE pool_name = ? AND name = ?", (pool_name, name))
        self.database.execute("DELETE FROM breakers WHERE pool_name = ? AND name = ?", (pool_name, name))
        return cursor.rowcount > 0
