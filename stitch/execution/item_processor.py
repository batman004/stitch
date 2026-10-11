import asyncio
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from ..errors import Park, StepFailed
from ..records import ItemRecord
from ..retry_policy import RetryPolicy
from ..serialization import to_json_text
from .context import ExecutionContext, ItemContext
from .run_session import RunSession


@dataclass
class ItemAttempt:
    result: Any = None
    should_retry: bool = False


class ItemProcessor:
    def __init__(
        self,
        session: RunSession,
        handler: Callable[[Any], Awaitable[Any]],
        key_function: Callable[[Any], str],
        item_retry_policy: RetryPolicy,
    ):
        self.session = session
        self.handler = handler
        self.key_function = key_function
        self.item_retry_policy = item_retry_policy
        self.store = session.store
        self.statistics = session.statistics

    async def process(self, value: Any) -> Any:
        item_key = self.key_function(value)
        existing = self.store.items.get(self.session.run_id, item_key)
        if existing is not None and existing.status == "done":
            return self._skip_finished_item(item_key, existing)
        input_text = to_json_text(value)
        previous_status = existing.status if existing else None
        attempt = 0
        while True:
            attempt += 1
            outcome = await self._attempt(value, item_key, input_text, attempt, previous_status)
            if not outcome.should_retry:
                return outcome.result

    async def _attempt(self, value: Any, item_key: str, input_text: str | None, attempt: int, previous_status: str | None) -> ItemAttempt:
        item_context = ItemContext(item_key)
        token = ExecutionContext.bind_item(item_context)
        self._record_item_start(item_key, input_text, attempt, previous_status)
        started_at = time.monotonic()
        try:
            result = await self.handler(value)
        except Park as error:
            return ItemAttempt(self._park_item(item_key, input_text, error))
        except StepFailed as error:
            return await self._handle_step_failure(item_key, error, attempt)
        except Exception as error:
            return ItemAttempt(self._fail_item(item_key, f"{type(error).__name__}: {error}"))
        finally:
            ExecutionContext.release_item(token)
        return ItemAttempt(self._finish_item(item_key, result, item_context, time.monotonic() - started_at))

    async def _handle_step_failure(self, item_key: str, error: StepFailed, attempt: int) -> ItemAttempt:
        if not self._should_retry_item(error, attempt):
            return ItemAttempt(self._fail_item(item_key, str(error)))
        await self._wait_before_item_retry(item_key, attempt, error)
        return ItemAttempt(should_retry=True)

    def _skip_finished_item(self, item_key: str, existing: ItemRecord) -> Any:
        self.statistics.items_skipped += 1
        self.session.emit("item_skipped", item=item_key, degraded=existing.degraded)
        return existing.result

    def _record_item_start(self, item_key: str, input_text: str | None, attempt: int, previous_status: str | None) -> None:
        self.store.items.mark_running(self.session.run_id, item_key, input_text)
        self.session.emit(
            "item_started", item=item_key, attempt=attempt, resumed=previous_status is not None, was=previous_status
        )

    def _should_retry_item(self, error: StepFailed, attempt: int) -> bool:
        return error.transient and attempt < self.item_retry_policy.maximum_attempts

    async def _wait_before_item_retry(self, item_key: str, attempt: int, error: StepFailed) -> None:
        delay_seconds = self.item_retry_policy.delay_before_next_attempt(attempt, error.cause)
        self.statistics.item_retries += 1
        self.session.emit(
            "item_retry", item=item_key, attempt=attempt, attempts=self.item_retry_policy.maximum_attempts,
            delay=delay_seconds, error=str(error),
        )
        await asyncio.sleep(delay_seconds)

    def _park_item(self, item_key: str, input_text: str | None, error: Park) -> None:
        if input_text is None:
            return self._fail_item(item_key, f"cannot park, input is not JSON: {error.reason}")
        self.store.items.mark_parked(self.session.run_id, item_key, error.until, error.reason)
        self.statistics.items_parked += 1
        self.session.emit("item_parked", item=item_key, until=error.until, reason=error.reason)

    def _fail_item(self, item_key: str, error_text: str) -> None:
        self.store.items.mark_failed(self.session.run_id, item_key, error_text)
        self.statistics.items_failed += 1
        self.session.emit("item_failed", item=item_key, error=error_text)

    def _finish_item(self, item_key: str, result: Any, item_context: ItemContext, elapsed_seconds: float) -> Any:
        self.store.items.mark_done(self.session.run_id, item_key, to_json_text(result), item_context.degraded)
        self.statistics.items_done += 1
        if item_context.degraded:
            self.statistics.items_degraded += 1
        self.session.emit("item_done", item=item_key, elapsed=elapsed_seconds, degraded=item_context.degraded)
        return result
