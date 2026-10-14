import functools
from typing import Any, Callable

from ..critical import CriticalPolicy, CriticalRegistry
from ..retry_policy import DEFAULT_RETRY_POLICY, RetryPolicy
from .context import ExecutionContext
from .step_definition import StepDefinition
from .step_executor import StepExecutor


def step(
    function: Callable[..., Any] | None = None,
    *,
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    timeout_seconds: float | None = None,
    name: str | None = None,
    degrade: Callable[..., Any] | None = None,
    critical: CriticalPolicy | bool = False,
):
    """Wrap an async function so a workflow can retry it and remember its result.

    Write ``@step`` on its own, or pass options: ``@step(retry_policy=..., timeout_seconds=..., name=..., degrade=..., critical=...)``.

    The decorator does not run the function. It returns a wrapper. Each call of that wrapper checks whether a workflow run is in progress:

    * With no run, the original function is called as usual.
    * With a run, ``StepExecutor`` takes over. It skips the call when a saved result already exists, retries errors from ``retry_policy``, and writes the new result to the checkpoint store.

    ``timeout_seconds`` is the limit for one try. ``name`` is the name stored with the checkpoint; the function name is used when it is left out. ``degrade`` is a backup function used when retries run out or no provider is free. ``critical`` makes a lasting failure stop the whole run. A step cannot be both critical and have a ``degrade`` backup.
    """

    def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
        """Turn ``target`` into a checkpointed step using the options passed to ``@step``.

        A critical step is also recorded in ``CriticalRegistry``, so the runner can look up its health check before starting work.
        """
        policy = resolve_critical_policy(critical, degrade)
        definition = StepDefinition(
            name or target.__name__,
            target,
            retry_policy,
            timeout_seconds,
            degrade,
            policy,
        )
        if policy is not None:
            CriticalRegistry.default().register(definition.name, policy)
        return build_checkpointed_step(definition)

    if function is None:
        return decorate
    return decorate(function)


def resolve_critical_policy(
    critical: CriticalPolicy | bool, degrade: Callable[..., Any] | None
) -> CriticalPolicy | None:
    """Decide whether this step should stop the run when it fails for good.

    ``False`` means the step is ordinary. ``True`` uses a plain ``CriticalPolicy``. A ``CriticalPolicy`` object is kept as given. A critical step cannot also have a ``degrade`` function, because a backup result would hide the failure that should stop the run.
    """
    if not critical:
        return None
    if degrade is not None:
        raise ValueError(
            "a critical step cannot also degrade: a fallback would hide the failure it must stop on"
        )
    return critical if isinstance(critical, CriticalPolicy) else CriticalPolicy()


def build_checkpointed_step(definition: StepDefinition) -> Callable[..., Any]:
    """Build the async function that ``@step`` puts in place of the original.

    The new function keeps the original name and docstring, and stores ``step_name`` so callers can see which step it is.
    """

    @functools.wraps(definition.function)
    async def checkpointed_step(*args, **kwargs):
        """Run the step, with checkpoints only when a workflow is active.

        ``ExecutionContext.current_run()`` is set only while a workflow is processing items. With no active run, the original function runs alone. With a run, ``StepExecutor`` handles skip, retry, backup, and saving.
        """
        session = ExecutionContext.current_run()
        if session is None:
            return await definition.function(*args, **kwargs)
        return await StepExecutor(session).execute(definition, args, kwargs)

    checkpointed_step.step_name = definition.name
    return checkpointed_step
