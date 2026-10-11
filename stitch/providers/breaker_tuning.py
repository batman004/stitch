from dataclasses import dataclass


@dataclass(frozen=True)
class BreakerTuning:
    failure_threshold: int = 3
    base_cooldown_seconds: float = 10.0
    maximum_cooldown_seconds: float = 120.0
    quota_cooldown_seconds: float = 60.0
    rate_limit_cooldown_seconds: float = 1.0
