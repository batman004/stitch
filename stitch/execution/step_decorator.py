import functools
from typing import Any, Callable

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
):
    def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
        definition = StepDefinition(name or target.__name__, target, retry_policy, timeout_seconds, degrade)
        return build_checkpointed_step(definition)

    if function is None:
        return decorate
    return decorate(function)


def build_checkpointed_step(definition: StepDefinition) -> Callable[..., Any]:
    @functools.wraps(definition.function)
    async def checkpointed_step(*args, **kwargs):
        session = ExecutionContext.current_run()
        if session is None:
            return await definition.function(*args, **kwargs)
        return await StepExecutor(session).execute(definition, args, kwargs)

    checkpointed_step.step_name = definition.name
    return checkpointed_step
