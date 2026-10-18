"""Durable, resumable async workflows backed by a single SQLite file.

Mark each stage of a pipeline with ``@step`` and run the per-item function through ``Workflow.run_many``. Every step result is checkpointed, so a rerun skips finished work. ``RetryPolicy`` retries failures, ``degrade`` supplies a fallback, ``ProviderPool`` fails over between backends, and ``WorkflowRecovery`` resumes or heals earlier runs.
"""

from .alerts import Alert, Alerter
from .critical import CriticalPolicy, CriticalRegistry
from .errors import (
    CriticalStepFailed,
    HealthCheckFailed,
    NoProviderAvailable,
    Park,
    PermanentError,
    PipelineHalted,
    ProviderDown,
    ProviderError,
    QuotaExhausted,
    RateLimited,
    StepFailed,
    TransientError,
)
from .execution.context import ExecutionContext
from .execution.reporting import Reporter, RunStatistics
from .execution.step_decorator import step
from .providers.breaker_tuning import BreakerTuning
from .providers.provider_pool import ProviderPool
from .records import AuditRecord
from .retry_policy import DEFAULT_RETRY_POLICY, NO_RETRY, RetryPolicy
from .storage.checkpoint_store import CheckpointStore
from .tracing import TraceRenderer
from .workflows.recovery import WorkflowRecovery
from .workflows.registry import WorkflowRegistry
from .workflows.workflow import Workflow, workflow

__version__ = "0.1.0"

__all__ = [
    "Alert",
    "Alerter",
    "CriticalPolicy",
    "CriticalRegistry",
    "CriticalStepFailed",
    "HealthCheckFailed",
    "PipelineHalted",
    "AuditRecord",
    "BreakerTuning",
    "CheckpointStore",
    "DEFAULT_RETRY_POLICY",
    "ExecutionContext",
    "NO_RETRY",
    "NoProviderAvailable",
    "Park",
    "PermanentError",
    "ProviderDown",
    "ProviderError",
    "ProviderPool",
    "QuotaExhausted",
    "RateLimited",
    "Reporter",
    "RetryPolicy",
    "RunStatistics",
    "StepFailed",
    "TraceRenderer",
    "TransientError",
    "Workflow",
    "WorkflowRecovery",
    "WorkflowRegistry",
    "step",
    "workflow",
]
