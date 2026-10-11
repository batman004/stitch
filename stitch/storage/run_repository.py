import json
import time

from .database import Database


class RunRepository:
    def __init__(self, database: Database):
        self.database = database

    def open_run(self, run_id: str, workflow_name: str, parameters: dict) -> str | None:
        existing = self.database.fetch_one("SELECT status FROM runs WHERE run_id = ?", (run_id,))
        now = time.time()
        if existing is None:
            self.database.execute(
                "INSERT INTO runs VALUES (?, ?, 'running', ?, ?, ?)",
                (run_id, workflow_name, json.dumps(parameters), now, now),
            )
            return None
        self.database.execute(
            "UPDATE runs SET status = 'running', updated_at = ? WHERE run_id = ?", (now, run_id)
        )
        return existing[0]

    def finish_run(self, run_id: str) -> None:
        self.database.execute(
            "UPDATE runs SET status = 'done', updated_at = ? WHERE run_id = ?", (time.time(), run_id)
        )

    def list_runs(self) -> list[tuple[str, str, str]]:
        return self.database.fetch_all(
            "SELECT run_id, workflow, status FROM runs ORDER BY created_at"
        )
