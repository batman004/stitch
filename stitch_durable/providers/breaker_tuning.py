from dataclasses import dataclass


@dataclass(frozen=True)
class BreakerTuning:
    """When a ``ProviderPool`` breaker opens and how long it stays open.

    ``failure_threshold`` is how many failures in a row (``ProviderDown``, ``ConnectionError``, timeouts) open the breaker. The cooldown starts at ``base_cooldown_seconds`` and doubles each time the breaker reopens, up to ``maximum_cooldown_seconds``. ``QuotaExhausted`` and ``RateLimited`` open the breaker on the first failure, for ``quota_cooldown_seconds`` and ``rate_limit_cooldown_seconds``, unless the error carries its own ``retry_after``.
    """

    failure_threshold: int = 3
    base_cooldown_seconds: float = 10.0
    maximum_cooldown_seconds: float = 120.0
    quota_cooldown_seconds: float = 60.0
    rate_limit_cooldown_seconds: float = 1.0
