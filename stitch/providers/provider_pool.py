import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Sequence

from ..errors import (
    NoProviderAvailable,
    ProviderDown,
    ProviderError,
    QuotaExhausted,
    RateLimited,
    TransientError,
    describe_error,
)
from ..execution.context import ExecutionContext
from ..records import BreakerRecord, ProviderRecord
from ..storage.checkpoint_store import CheckpointStore
from .breaker_tuning import BreakerTuning


@dataclass
class ProviderAttempt:
    succeeded: bool
    result: Any = None
    reopen_at: float | None = None


class FailoverTracker:
    def __init__(self):
        self.failed_over_from: str | None = None
        self.cause = "cooling"
        self.earliest_retry_at: float | None = None

    def record_cooling_provider(self, name: str, retry_at: float | None) -> None:
        self._remember_source(name)
        self._remember_retry_time(retry_at)

    def record_failure(self, name: str, reopen_at: float | None) -> None:
        self._remember_source(name)
        self.cause = "failed"
        self._remember_retry_time(reopen_at)

    def _remember_source(self, name: str) -> None:
        if self.failed_over_from is None:
            self.failed_over_from = name

    def _remember_retry_time(self, retry_at: float | None) -> None:
        if retry_at is None:
            return
        if self.earliest_retry_at is None or retry_at < self.earliest_retry_at:
            self.earliest_retry_at = retry_at


class ProviderPool:
    def __init__(
        self,
        name: str,
        client_factory: Callable[[str, dict], Any],
        default_providers: Sequence[tuple[str, int, dict]] = (),
        tuning: BreakerTuning = BreakerTuning(),
    ):
        self.name = name
        self.client_factory = client_factory
        self.default_providers = list(default_providers)
        self.tuning = tuning
        self._clients: dict[tuple[str, str], Any] = {}
        self._providers_being_probed: set[str] = set()

    def seed_defaults(self, store: CheckpointStore) -> None:
        if store.providers.list_providers(self.name):
            return
        for name, priority, config in self.default_providers:
            store.providers.save(self.name, name, priority, config)

    def register(self, store: CheckpointStore, name: str, priority: int, config: dict) -> None:
        store.providers.save(self.name, name, priority, config)
        store.audit.record(None, "provider_registered", {"provider": name, "pool": self.name, "priority": priority, **config})

    async def call(self, operation: Callable[[Any], Awaitable[Any]]) -> Any:
        store = ExecutionContext.require_store()
        self.seed_defaults(store)
        providers = [provider for provider in store.providers.list_providers(self.name) if provider.enabled]
        tracker = FailoverTracker()
        for provider in providers:
            breaker = store.breakers.get(self.name, provider.name)
            if self._is_cooling_down(breaker):
                tracker.record_cooling_provider(provider.name, breaker.retry_at)
                continue
            probing = self._is_ready_for_probe(breaker)
            if probing and provider.name in self._providers_being_probed:
                continue
            attempt = await self._attempt_provider(store, provider, breaker, operation, probing)
            if attempt.succeeded:
                self._report_success(provider.name, tracker)
                return attempt.result
            tracker.record_failure(provider.name, attempt.reopen_at)
        raise self._no_provider_error(providers, tracker)

    def snapshot(self, store: CheckpointStore) -> list[dict]:
        breakers = store.breakers.list_breakers(self.name)
        now = time.time()
        return [self._describe_provider(provider, breakers.get(provider.name), now) for provider in store.providers.list_providers(self.name)]

    async def _attempt_provider(self, store, provider: ProviderRecord, breaker, operation, probing: bool) -> ProviderAttempt:
        if probing:
            self._providers_being_probed.add(provider.name)
            ExecutionContext.report_event("breaker_probe", provider=provider.name)
        try:
            result = await operation(self._client_for(provider))
        except ProviderError as error:
            return self._record_failure(store, provider.name, breaker, error, probing)
        except (TransientError, TimeoutError, ConnectionError) as error:
            return self._record_failure(store, provider.name, breaker, ProviderDown(describe_error(error)), probing)
        finally:
            self._providers_being_probed.discard(provider.name)
        self._record_success(store, provider.name, breaker)
        return ProviderAttempt(succeeded=True, result=result)

    def _record_failure(self, store, name: str, breaker, error: ProviderError, probing: bool) -> ProviderAttempt:
        failures = (breaker.failures if breaker else 0) + 1
        consecutive_opens = breaker.consecutive_opens if breaker else 0
        reason = describe_error(error)
        ExecutionContext.report_event("provider_failed", provider=name, error=reason, kind=type(error).__name__)
        cooldown_seconds = self._cooldown_seconds(error, failures, consecutive_opens, probing)
        if cooldown_seconds is None:
            store.breakers.save(BreakerRecord(self.name, name, "closed", failures, consecutive_opens, None, reason))
            return ProviderAttempt(succeeded=False)
        reopen_at = time.time() + cooldown_seconds
        store.breakers.save(BreakerRecord(self.name, name, "open", failures, consecutive_opens + 1, reopen_at, reason))
        ExecutionContext.report_event(
            "breaker_opened", provider=name, reason=reason, cooldown=cooldown_seconds, until=reopen_at
        )
        return ProviderAttempt(succeeded=False, reopen_at=reopen_at)

    def _record_success(self, store, name: str, breaker) -> None:
        if breaker is None or (breaker.state == "closed" and breaker.failures == 0):
            return
        store.breakers.save(BreakerRecord(self.name, name, "closed", 0, 0, None, None))
        if breaker.state == "open":
            ExecutionContext.report_event("breaker_closed", provider=name)

    def _cooldown_seconds(self, error: ProviderError, failures: int, consecutive_opens: int, probing: bool) -> float | None:
        if isinstance(error, QuotaExhausted):
            return error.retry_after or self.tuning.quota_cooldown_seconds
        if isinstance(error, RateLimited):
            return error.retry_after or self.tuning.rate_limit_cooldown_seconds
        if probing or failures >= self.tuning.failure_threshold:
            return min(self.tuning.maximum_cooldown_seconds, self.tuning.base_cooldown_seconds * 2**consecutive_opens)
        return None

    def _report_success(self, name: str, tracker: FailoverTracker) -> None:
        if tracker.failed_over_from is None:
            ExecutionContext.report_event("provider_used", provider=name)
            return
        ExecutionContext.report_event(
            "failover", from_provider=tracker.failed_over_from, provider=name, cause=tracker.cause
        )

    def _no_provider_error(self, providers: list[ProviderRecord], tracker: FailoverTracker) -> NoProviderAvailable:
        if not providers:
            return NoProviderAvailable(f"pool {self.name!r} has no providers registered")
        retry_at = tracker.earliest_retry_at or time.time() + self.tuning.base_cooldown_seconds
        names = ", ".join(provider.name for provider in providers)
        return NoProviderAvailable(f"no provider available in pool {self.name!r} ({names} all cooling down)", retry_at=retry_at)

    def _is_cooling_down(self, breaker: BreakerRecord | None) -> bool:
        if breaker is None or breaker.state != "open":
            return False
        return breaker.retry_at is not None and time.time() < breaker.retry_at

    def _is_ready_for_probe(self, breaker: BreakerRecord | None) -> bool:
        return breaker is not None and breaker.state == "open" and not self._is_cooling_down(breaker)

    def _client_for(self, provider: ProviderRecord) -> Any:
        cache_key = (provider.name, repr(sorted(provider.config.items())))
        if cache_key not in self._clients:
            self._clients[cache_key] = self.client_factory(provider.name, provider.config)
        return self._clients[cache_key]

    def _describe_provider(self, provider: ProviderRecord, breaker: BreakerRecord | None, now: float) -> dict:
        retry_in = max(0.0, breaker.retry_at - now) if breaker and breaker.retry_at else 0.0
        return {
            "name": provider.name,
            "priority": provider.priority,
            "state": self._provider_state(provider, breaker, now),
            "config": provider.config,
            "reason": breaker.reason if breaker else None,
            "retry_in": retry_in,
            "failures": breaker.failures if breaker else 0,
        }

    def _provider_state(self, provider: ProviderRecord, breaker: BreakerRecord | None, now: float) -> str:
        if not provider.enabled:
            return "disabled"
        if breaker is None or breaker.state != "open":
            return "ready"
        if breaker.retry_at and breaker.retry_at > now:
            return "cooling"
        return "probe"
