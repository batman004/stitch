import uuid

from .execution.context import ExecutionContext

IDEMPOTENCY_NAMESPACE = uuid.UUID("9388f415-f88c-486c-89a4-ec0bd295ac05")
FIELD_SEPARATOR = "\x1f"


def idempotency_key(scope: str = "") -> str:
    """Return a stable key for the step that is running now, to send as an ``Idempotency-Key``.

    The key is a UUID built from the run id, the item key and the step's checkpoint name. It is the same on every retry of the step and on every rerun of the run, so a provider that honours idempotency keys answers a repeat with its first result instead of acting twice. This closes the one gap a checkpoint cannot: a crash after the provider acted but before the step's checkpoint was written.

    Example::

        @step(retry_policy=RetryPolicy(retry_on=(httpx.TransportError,)))
        async def charge(customer):
            response = await payments.post(
                "/v1/payments",
                json=payment_body(customer),
                headers={"Idempotency-Key": idempotency_key()},
            )
            return response.json()

    A different ``batch_label`` is a different run, so next month's billing run gets new keys. A step called twice in one item (stored as ``name`` and ``name#2``) gets a different key for each call. Pass ``scope`` when one step makes several requests that each need their own key, for example ``idempotency_key("refund")``.

    ``WorkflowRecovery.heal()`` reruns a degraded step under the same name, so it sends the same key again. That is what you want when the first request may have succeeded; if the provider stored a failed response under that key, pass a ``scope`` to start fresh.

    Raises ``RuntimeError`` outside a step that a workflow is running, because there is nothing stable to build a key from. A ``degrade`` fallback also runs outside the step.
    """
    current = ExecutionContext.current_step()
    if current is None:
        raise RuntimeError(
            "idempotency_key() only works inside a @step while a workflow is running it; "
            "outside a run there is no run, item and step to build a stable key from"
        )
    name = FIELD_SEPARATOR.join(
        (current.run_id, current.item_key, current.step_name, scope)
    )
    return str(uuid.uuid5(IDEMPOTENCY_NAMESPACE, name))
