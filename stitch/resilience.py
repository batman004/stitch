"""Providers, circuit breakers and failover.

    llm = Pool("llm", factory=make_client, defaults=[("eu-west-1", 1, {...})])

    summary = await llm.call(lambda client: client.complete(prompt))

A Pool holds an ordered list of interchangeable providers (LLM regions, payment
processors, SMS gateways...). `call` tries them in priority order and:

  * skips a provider whose breaker is open (it is cooling down)
  * fails over to the next provider when one fails
  * opens a breaker when a provider is out of credits, rate limited, or keeps failing
  * lets a cooled-down provider back in with a single probe call (half-open)
  * raises NoProviderAvailable, with the time the first breaker reopens, when
    nothing is left. The engine turns that into a degraded result or a parked item.

Both the registry and the breakers live in the Store, not in memory. That has
two consequences worth the trouble:

  * a cooldown survives a restart: crash during an outage and the new process
    still knows eu-west-1 is out of credits for another 40 seconds;
  * you can register a new provider while the pipeline is running, from another
    process, and the next call picks it up.

Providers are described by JSON config (a name, a URL, a region), because code
can't be stored. `factory(name, config)` turns the config into a client object.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

from .engine import current_store, emit_current
from .errors import (
    NoProviderAvailable,
    ProviderDown,
    ProviderError,
    QuotaExhausted,
    RateLimited,
    TransientError,
    describe,
)
from .store import ProviderRow, Store

T = TypeVar("T")


def _soonest(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return b if a is None else a
    return min(a, b)


@dataclass(frozen=True)
class Defaults:
    """Breaker tuning. Seconds, because the demo runs in seconds; scale for production."""

    failure_threshold: int = 3      # consecutive failures before a breaker opens
    cooldown: float = 10.0          # first open period after repeated failures
    max_cooldown: float = 120.0     # exponential backoff stops here
    quota_cooldown: float = 60.0    # out of credits: long, nothing short will fix it
    rate_limit_cooldown: float = 1.0


class Pool:
    def __init__(
        self,
        name: str,
        factory: Callable[[str, dict], Any],
        *,
        defaults: Sequence[tuple[str, int, dict]] = (),
        tuning: Defaults = Defaults(),
    ):
        self.name = name
        self.factory = factory
        self.seed = list(defaults)
        self.tuning = tuning
        self._clients: dict[tuple[str, str], Any] = {}
        self._probing: set[str] = set()

    # -- registry ---------------------------------------------------------

    def seed_defaults(self, store: Store) -> None:
        """Write the built-in providers if the registry is empty."""
        if not store.list_providers(self.name):
            for name, priority, config in self.seed:
                store.put_provider(self.name, name, priority, config)

    def register(self, store: Store, name: str, priority: int, config: dict) -> None:
        """Add or update a provider. Takes effect on the next call, in any process."""
        store.put_provider(self.name, name, priority, config)
        store.audit(None, "provider_registered", {"provider": name, "pool": self.name,
                                                  "priority": priority, **config})

    def _client(self, row: ProviderRow) -> Any:
        key = (row.name, repr(sorted(row.config.items())))
        if key not in self._clients:
            self._clients[key] = self.factory(row.name, row.config)
        return self._clients[key]

    # -- calling ----------------------------------------------------------

    async def call(self, fn: Callable[[Any], Awaitable[T]]) -> T:
        store = current_store()
        self.seed_defaults(store)
        providers = [p for p in store.list_providers(self.name) if p.enabled]
        if not providers:
            raise NoProviderAvailable(f"pool {self.name!r} has no providers registered")

        failed_over_from: str | None = None
        cause = "cooling"   # "failed" once a provider actually failed during this call
        earliest: float | None = None
        for row in providers:
            breaker = store.get_breaker(self.name, row.name)
            now = time.time()
            probe = False
            if breaker is not None and breaker.state == "open":
                if breaker.retry_at is not None and now < breaker.retry_at:
                    earliest = _soonest(earliest, breaker.retry_at)
                    failed_over_from = failed_over_from or row.name
                    continue
                if row.name in self._probing:  # someone else is already testing it
                    continue
                probe = True
                self._probing.add(row.name)
                emit_current("breaker_probe", provider=row.name)

            try:
                result = await fn(self._client(row))
            except ProviderError as exc:
                opened = self._failed(store, row.name, breaker, exc, probe)
                earliest = _soonest(earliest, opened)
                failed_over_from, cause = failed_over_from or row.name, "failed"
                continue
            except (TransientError, TimeoutError, ConnectionError) as exc:
                opened = self._failed(store, row.name, breaker, ProviderDown(describe(exc)), probe)
                earliest = _soonest(earliest, opened)
                failed_over_from, cause = failed_over_from or row.name, "failed"
                continue
            finally:
                if probe:
                    self._probing.discard(row.name)

            self._succeeded(store, row.name, breaker)
            if failed_over_from is not None:
                emit_current("failover", from_provider=failed_over_from, provider=row.name,
                              cause=cause)
            else:
                emit_current("provider_used", provider=row.name)
            return result

        retry_at = earliest if earliest is not None else time.time() + self.tuning.cooldown
        raise NoProviderAvailable(
            f"no provider available in pool {self.name!r} "
            f"({', '.join(p.name for p in providers)} all cooling down)", retry_at=retry_at)

    # -- breaker bookkeeping ------------------------------------------------

    def _failed(self, store: Store, name: str, breaker, exc: ProviderError, probe: bool) -> float | None:
        """Record a failure. Returns when the breaker reopens, if this failure opened it."""
        t = self.tuning
        failures = (breaker.failures if breaker else 0) + 1
        opens = breaker.opens if breaker else 0
        now = time.time()
        reason = describe(exc)
        emit_current("provider_failed", provider=name, error=reason, kind=type(exc).__name__)

        if isinstance(exc, QuotaExhausted):
            cooldown = exc.retry_after or t.quota_cooldown
        elif isinstance(exc, RateLimited):
            cooldown = exc.retry_after or t.rate_limit_cooldown
        elif probe or failures >= t.failure_threshold:
            cooldown = min(t.max_cooldown, t.cooldown * 2 ** opens)
        else:
            store.put_breaker(self.name, name, "closed", failures, opens, None, reason)
            return None

        store.put_breaker(self.name, name, "open", failures, opens + 1, now + cooldown, reason)
        emit_current("breaker_opened", provider=name, reason=reason,
                     cooldown=cooldown, until=now + cooldown)
        return now + cooldown

    def _succeeded(self, store: Store, name: str, breaker) -> None:
        if breaker is None or (breaker.state == "closed" and breaker.failures == 0):
            return
        store.put_breaker(self.name, name, "closed", 0, 0, None, None)
        if breaker.state == "open":
            emit_current("breaker_closed", provider=name)

    # -- inspection ---------------------------------------------------------

    def snapshot(self, store: Store) -> list[dict]:
        """Providers with their live breaker state, in the order they would be tried."""
        breakers = store.breakers(self.name)
        now = time.time()
        out = []
        for row in store.list_providers(self.name):
            b = breakers.get(row.name)
            state = "ready"
            if not row.enabled:
                state = "disabled"
            elif b is not None and b.state == "open":
                state = "cooling" if b.retry_at and b.retry_at > now else "probe"
            out.append({"name": row.name, "priority": row.priority, "state": state,
                        "config": row.config, "reason": b.reason if b else None,
                        "retry_in": max(0.0, b.retry_at - now) if b and b.retry_at else 0.0,
                        "failures": b.failures if b else 0})
        return out
