"""stitch: durable, resumable Python pipelines from the standard library alone.

    @stitch.workflow(key=lambda job: job["id"])
    async def pipeline(job): ...        # survives crashes, resumes where it stopped

    @stitch.step(retry=stitch.Retry(attempts=5), degrade=fallback)
    async def call_vendor(job): ...     # checkpointed, retried, with a failsafe

    llm = stitch.Pool("llm", factory=make_client)   # fallback providers with circuit breakers
"""

from .engine import (
    NO_RETRY,
    Crash,
    Reporter,
    Retry,
    Stats,
    StepContext,
    current_step,
    idempotency_key,
    run_batch,
    step,
)
from .errors import (
    NoProviderAvailable,
    Park,
    PermanentError,
    ProviderDown,
    ProviderError,
    QuotaExhausted,
    RateLimited,
    StepFailed,
    TransientError,
    WorkflowFailed,
    WorkflowParked,
)
from .resilience import Defaults, Pool
from .store import AuditRow, NullStore, StepRecord, Store
from .trace import render as render_trace
from .flow import Workflow, configure, heal, resume, workflow

__all__ = [
    "AuditRow", "Crash", "Defaults", "NO_RETRY", "NoProviderAvailable", "NullStore", "Park",
    "PermanentError", "Pool", "ProviderDown", "ProviderError", "QuotaExhausted", "RateLimited",
    "Reporter", "Retry", "Stats", "StepContext", "StepFailed", "StepRecord", "Store",
    "TransientError", "Workflow", "WorkflowFailed", "WorkflowParked", "configure", "current_step",
    "heal", "idempotency_key", "render_trace", "resume", "run_batch", "step", "workflow",
]
