import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Callable

from .errors import describe_error


@dataclass(frozen=True)
class CriticalPolicy:
    """Makes a step critical: when it fails after its retries, the whole run halts instead of failing items one by one.

    ``health_check`` is a sync or async function with no arguments. Before a halted run continues, it is called; returning ``False`` or raising keeps the run halted. ``health_check_timeout_seconds`` limits how long it may take.

    Example::

        @step(critical=CriticalPolicy(health_check=database_ok))
        async def save(item, text): ...
    """

    health_check: Callable[[], Any] | None = None
    health_check_timeout_seconds: float = 10.0

    async def verify(self) -> str | None:
        """Run the health check. Return ``None`` when it passes, otherwise the reason it failed."""
        health_check = self.health_check
        if health_check is None:
            return None
        try:
            async with asyncio.timeout(self.health_check_timeout_seconds):
                outcome = await self._call_health_check(health_check)
        except Exception as error:
            return describe_error(error)
        return "the health check returned False" if outcome is False else None

    async def _call_health_check(self, health_check: Callable[[], Any]) -> Any:
        if inspect.iscoroutinefunction(health_check):
            return await health_check()
        return await asyncio.to_thread(health_check)


class CriticalRegistry:
    """Maps critical step names to their ``CriticalPolicy``. ``@step`` fills it, and the runner reads it to health-check a halted run."""

    _default = None

    def __init__(self):
        """Create an empty registry."""
        self._policies: dict[str, CriticalPolicy] = {}

    @classmethod
    def default(cls) -> "CriticalRegistry":
        """Return the process-wide registry, creating it on first use."""
        if cls._default is None:
            cls._default = cls()
        return cls._default

    def register(self, step_name: str, policy: CriticalPolicy) -> None:
        """Store ``policy`` for ``step_name``, replacing any earlier one."""
        self._policies[step_name] = policy

    def find(self, step_name: str) -> CriticalPolicy | None:
        """Return the policy for ``step_name``, or ``None`` if the step is not critical."""
        return self._policies.get(step_name)
