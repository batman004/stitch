import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any

from ..errors import NoProviderAvailable, Park, StepFailed, TransientError, describe_error
from ..records import StepRecord
from .context import ExecutionContext, ItemContext, StepContext, StepIdentity
from .run_session import RunSession
from .step_definition import StepDefinition

COMPLETED_STATUSES = ("done", "degraded")
DEFAULT_PARK_SECONDS = 5.0


@dataclass
class StepOutcome:
    attempts: int
    result: Any = None
    degraded_reason: str | None = None
    degraded_cause: BaseException | None = None


class StepExecutor:
    def __init__(self, session: RunSession):
        self.session = session
        self.store = session.store

    async def execute(self, definition: StepDefinition, args: tuple, kwargs: dict) -> Any:
        item_context = ExecutionContext.current_item()
        identity = item_context.register_step_call(definition.name)
        previous = self.store.steps.get(self.session.run_id, identity.item_key, identity.step_id)
        if previous is not None and previous.status in COMPLETED_STATUSES:
            return self._reuse_checkpoint(item_context, identity, previous)
        self._record_start(identity, previous)
        started_at = time.monotonic()
        outcome = await self._run_attempts(definition, identity, args, kwargs)
        if outcome.degraded_reason is not None:
            outcome.result = await self._produce_fallback(definition, identity, args, kwargs, outcome)
            self._record_degradation(item_context, identity, outcome)
        self._record_completion(identity, outcome, time.monotonic() - started_at)
        return outcome.result

    def _reuse_checkpoint(self, item_context: ItemContext, identity: StepIdentity, previous: StepRecord) -> Any:
        degraded = previous.status == "degraded"
        if degraded:
            item_context.degraded = True
        self.session.statistics.steps_skipped += 1
        self.session.emit(
            "step_skipped", item=identity.item_key, step=identity.step_id, result=previous.result, degraded=degraded
        )
        return previous.result

    def _record_start(self, identity: StepIdentity, previous: StepRecord | None) -> None:
        interrupted = previous is not None and previous.status == "running"
        self.store.steps.mark_started(self.session.run_id, identity.item_key, identity.step_id, identity.sequence)
        self.session.emit("step_started", item=identity.item_key, step=identity.step_id, interrupted=interrupted)

    def _record_degradation(self, item_context: ItemContext, identity: StepIdentity, outcome: StepOutcome) -> None:
        item_context.degraded = True
        self.session.statistics.steps_degraded += 1
        self.session.emit(
            "step_degraded", item=identity.item_key, step=identity.step_id, reason=outcome.degraded_reason
        )

    def _record_completion(self, identity: StepIdentity, outcome: StepOutcome, elapsed_seconds: float) -> None:
        degraded = outcome.degraded_reason is not None
        self.store.steps.mark_done(
            self.session.run_id, identity.item_key, identity.step_id, outcome.result, outcome.attempts, degraded
        )
        self.session.statistics.steps_run += 1
        self.session.emit(
            "step_done", item=identity.item_key, step=identity.step_id, result=outcome.result,
            attempts=outcome.attempts, elapsed=elapsed_seconds, degraded=degraded,
        )

    async def _run_attempts(self, definition: StepDefinition, identity: StepIdentity, args: tuple, kwargs: dict) -> StepOutcome:
        policy = definition.retry_policy
        attempt = 0
        while True:
            attempt += 1
            try:
                result = await self._call_once(definition, identity, attempt, args, kwargs)
                return StepOutcome(attempts=attempt, result=result)
            except NoProviderAvailable as error:
                return self._handle_missing_provider(definition, identity, attempt, error)
            except Park as error:
                self._record_waiting(identity, error, attempt)
                raise
            except policy.retry_on as error:
                if attempt >= policy.maximum_attempts:
                    return self._handle_exhausted_retries(definition, identity, attempt, error)
                await self._wait_before_retry(definition, identity, attempt, error)
            except Exception as error:
                raise self._fail_step(identity, error, attempt, isinstance(error, TransientError)) from error

    async def _call_once(self, definition: StepDefinition, identity: StepIdentity, attempt: int, args: tuple, kwargs: dict) -> Any:
        context = StepContext(self.session.run_id, identity.item_key, identity.step_id, attempt)
        token = ExecutionContext.bind_step(context)
        try:
            async with asyncio.timeout(definition.timeout_seconds):
                return await definition.function(*args, **kwargs)
        finally:
            ExecutionContext.release_step(token)

    def _handle_missing_provider(self, definition: StepDefinition, identity: StepIdentity, attempt: int, error: NoProviderAvailable) -> StepOutcome:
        if definition.degrade_function is None:
            self._record_waiting(identity, error, attempt)
            raise Park(error.retry_at or time.time() + DEFAULT_PARK_SECONDS, describe_error(error)) from error
        return StepOutcome(attempts=attempt, degraded_reason=describe_error(error), degraded_cause=error)

    def _handle_exhausted_retries(self, definition: StepDefinition, identity: StepIdentity, attempt: int, error: BaseException) -> StepOutcome:
        if definition.degrade_function is None:
            raise self._fail_step(identity, error, attempt, True) from error
        reason = f"retries exhausted: {describe_error(error)}"
        return StepOutcome(attempts=attempt, degraded_reason=reason, degraded_cause=error)

    async def _wait_before_retry(self, definition: StepDefinition, identity: StepIdentity, attempt: int, error: BaseException) -> None:
        policy = definition.retry_policy
        delay_seconds = policy.delay_before_next_attempt(attempt, error)
        self.session.statistics.step_retries += 1
        self.session.emit(
            "step_retry", item=identity.item_key, step=identity.step_id, attempt=attempt,
            attempts=policy.maximum_attempts, delay=delay_seconds, error=describe_error(error),
        )
        await asyncio.sleep(delay_seconds)

    async def _produce_fallback(self, definition: StepDefinition, identity: StepIdentity, args: tuple, kwargs: dict, outcome: StepOutcome) -> Any:
        fallback_kwargs = self._fallback_keyword_arguments(definition, kwargs, outcome.degraded_cause)
        try:
            result = definition.degrade_function(*args, **fallback_kwargs)
            if inspect.isawaitable(result):
                result = await result
            return result
        except Park as error:
            self._record_waiting(identity, error, outcome.attempts)
            raise

    def _fallback_keyword_arguments(self, definition: StepDefinition, kwargs: dict, cause: BaseException | None) -> dict:
        parameters = inspect.signature(definition.degrade_function).parameters
        if "cause" in parameters:
            return {**kwargs, "cause": cause}
        return kwargs

    def _fail_step(self, identity: StepIdentity, error: BaseException, attempt: int, transient: bool) -> StepFailed:
        self.store.steps.mark_failed(
            self.session.run_id, identity.item_key, identity.step_id, describe_error(error), attempt
        )
        self.session.emit(
            "step_failed", item=identity.item_key, step=identity.step_id, error=describe_error(error), attempts=attempt
        )
        return StepFailed(identity.step_id, error, transient)

    def _record_waiting(self, identity: StepIdentity, error: BaseException, attempt: int) -> None:
        self.store.steps.mark_waiting(
            self.session.run_id, identity.item_key, identity.step_id, describe_error(error), attempt
        )
        self.session.emit("step_waiting", item=identity.item_key, step=identity.step_id, reason=describe_error(error))
