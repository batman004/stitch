import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable

from .errors import describe_error


@dataclass(frozen=True)
class CriticalPolicy:
    health_check: Callable[[], Any] | None = None
    health_check_timeout_seconds: float = 10.0

    async def verify(self) -> str | None:
        if self.health_check is None:
            return None
        try:
            async with asyncio.timeout(self.health_check_timeout_seconds):
                outcome = await self._call_health_check()
        except Exception as error:
            return describe_error(error)
        return "the health check returned False" if outcome is False else None

    async def _call_health_check(self) -> Any:
        if inspect.iscoroutinefunction(self.health_check):
            return await self.health_check()
        return await asyncio.to_thread(self.health_check)


class CriticalRegistry:
    _default = None

    def __init__(self):
        self._policies: dict[str, CriticalPolicy] = {}

    @classmethod
    def default(cls) -> "CriticalRegistry":
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def register(self, step_name: str, policy: CriticalPolicy) -> None:
        self._policies[step_name] = policy

    def find(self, step_name: str) -> CriticalPolicy | None:
        return self._policies.get(step_name)
