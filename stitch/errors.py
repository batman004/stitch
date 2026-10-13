class TransientError(Exception):
    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    pass


class ProviderError(Exception):
    def __init__(self, message: str, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class QuotaExhausted(ProviderError):
    pass


class RateLimited(ProviderError):
    pass


class ProviderDown(ProviderError):
    pass


class NoProviderAvailable(Exception):
    def __init__(self, message: str, retry_at: float | None = None):
        super().__init__(message)
        self.retry_at = retry_at


class Park(Exception):
    def __init__(self, until: float, reason: str = ""):
        super().__init__(reason or "parked")
        self.until = until
        self.reason = reason


class StepFailed(Exception):
    def __init__(self, step_name: str, cause: BaseException, transient: bool):
        super().__init__(f"{step_name}: {describe_error(cause)}")
        self.step_name = step_name
        self.cause = cause
        self.transient = transient


class PipelineHalted(Exception):
    pass


class CriticalStepFailed(PipelineHalted):
    def __init__(self, step_name: str, item_key: str, cause: BaseException):
        super().__init__(f"critical step {step_name} failed for {item_key}: {describe_error(cause)}")
        self.step_name = step_name
        self.item_key = item_key
        self.cause = cause


class HealthCheckFailed(PipelineHalted):
    def __init__(self, step_name: str, reason: str):
        super().__init__(f"the run is halted and the resource behind {step_name} is still unhealthy: {reason}")
        self.step_name = step_name
        self.reason = reason


def describe_error(error: BaseException) -> str:
    if isinstance(error, TimeoutError):
        return "timed out"
    return str(error) or type(error).__name__
