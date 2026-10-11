from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StepRecord:
    item_key: str
    step_name: str
    sequence: int
    status: str
    attempts: int
    result: Any
    error: str | None


@dataclass(frozen=True)
class ItemRecord:
    status: str
    input_value: Any
    result: Any
    degraded: bool
    retry_at: float | None
    error: str | None


@dataclass(frozen=True)
class ItemInput:
    item_key: str
    input_value: Any


@dataclass(frozen=True)
class ParkedItem:
    item_key: str
    input_value: Any
    retry_at: float


@dataclass(frozen=True)
class AuditRecord:
    record_id: int
    timestamp: float
    run_id: str | None
    item_key: str | None
    step_name: str | None
    event: str
    attempt: int | None
    provider: str | None
    duration_seconds: float | None
    detail: dict


@dataclass(frozen=True)
class ProviderRecord:
    pool_name: str
    name: str
    priority: int
    enabled: bool
    config: dict


@dataclass(frozen=True)
class BreakerRecord:
    pool_name: str
    name: str
    state: str
    failures: int
    consecutive_opens: int
    retry_at: float | None
    reason: str | None
