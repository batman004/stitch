import sqlite3
from pathlib import Path

SCHEMA_VERSION = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    workflow TEXT NOT NULL,
    status TEXT NOT NULL,
    parameters TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    run_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    status TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    input TEXT,
    result TEXT,
    degraded INTEGER NOT NULL DEFAULT 0,
    retry_at REAL,
    error TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (run_id, item_key)
);
CREATE TABLE IF NOT EXISTS steps (
    run_id TEXT NOT NULL,
    item_key TEXT NOT NULL,
    step_name TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    error TEXT,
    started_at REAL NOT NULL,
    finished_at REAL,
    PRIMARY KEY (run_id, item_key, step_name)
);
CREATE TABLE IF NOT EXISTS audit (
    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp REAL NOT NULL,
    run_id TEXT,
    item_key TEXT,
    step_name TEXT,
    event TEXT NOT NULL,
    attempt INTEGER,
    provider TEXT,
    duration_seconds REAL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS audit_by_item ON audit (item_key, record_id);
CREATE TABLE IF NOT EXISTS providers (
    pool_name TEXT NOT NULL,
    name TEXT NOT NULL,
    priority INTEGER NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    config TEXT NOT NULL,
    registered_at REAL NOT NULL,
    PRIMARY KEY (pool_name, name)
);
CREATE TABLE IF NOT EXISTS breakers (
    pool_name TEXT NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0,
    consecutive_opens INTEGER NOT NULL DEFAULT 0,
    retry_at REAL,
    reason TEXT,
    updated_at REAL NOT NULL,
    PRIMARY KEY (pool_name, name)
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(str(self.path), isolation_level=None)
        self._configure_durability()
        self._ensure_schema()

    def execute(self, sql: str, parameters: tuple = ()) -> sqlite3.Cursor:
        return self.connection.execute(sql, parameters)

    def fetch_one(self, sql: str, parameters: tuple = ()) -> tuple | None:
        return self.execute(sql, parameters).fetchone()

    def fetch_all(self, sql: str, parameters: tuple = ()) -> list[tuple]:
        return self.execute(sql, parameters).fetchall()

    def close(self) -> None:
        self.connection.close()

    def _configure_durability(self) -> None:
        self.execute("PRAGMA journal_mode=WAL")
        self.execute("PRAGMA synchronous=NORMAL")
        self.execute("PRAGMA busy_timeout=2000")

    def _ensure_schema(self) -> None:
        stored_version = self.fetch_one("PRAGMA user_version")[0]
        has_tables = self.fetch_one("SELECT 1 FROM sqlite_master WHERE name = 'items'")
        if has_tables and stored_version != SCHEMA_VERSION:
            raise RuntimeError(
                f"{self.path} uses schema {stored_version} but this version needs {SCHEMA_VERSION}; delete the file to start fresh"
            )
        self.connection.executescript(SCHEMA)
        self.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
