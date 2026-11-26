import asyncio
import tempfile
import unittest
import uuid
from pathlib import Path

from stitch_durable import (
    CheckpointStore,
    PermanentError,
    RetryPolicy,
    TransientError,
    Workflow,
    WorkflowRegistry,
    idempotency_key,
    step,
)

FAST_RETRY = RetryPolicy(
    maximum_attempts=3, base_delay_seconds=0.001, maximum_delay_seconds=0.001
)


class KeyRecorder:
    """Stands in for a payment provider: notes the key each request carried, and can fail on cue."""

    def __init__(self):
        self.keys = []
        self.failures = []

    def charge(self, label):
        self.keys.append((label, idempotency_key()))
        if self.failures:
            raise self.failures.pop(0)


provider = KeyRecorder()


@step(retry_policy=FAST_RETRY)
async def charge(customer):
    provider.charge(f"charge:{customer}")
    return {"charged": customer}


@step(retry_policy=FAST_RETRY)
async def refund(customer):
    provider.charge(f"refund:{customer}")
    return {"refunded": customer}


@step(retry_policy=FAST_RETRY)
async def two_requests(customer):
    return {"create": idempotency_key("create"), "capture": idempotency_key("capture")}


async def bill(customer):
    return await charge(customer)


async def bill_twice(customer):
    await charge(customer)
    return await charge(customer)


async def charge_then_refund(customer):
    await charge(customer)
    return await refund(customer)


async def scoped(customer):
    return await two_requests(customer)


class IdempotencyTestCase(unittest.TestCase):
    def setUp(self):
        provider.keys.clear()
        provider.failures.clear()
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary_directory.name) / "state.db"
        self.stores = []

    def tearDown(self):
        for store in self.stores:
            store.close()
        self.temporary_directory.cleanup()

    def open_store(self):
        store = CheckpointStore(self.path)
        self.stores.append(store)
        return store

    def run_workflow(self, handler, items, name="billing", **options):
        workflow = Workflow(handler, name=name, registry=WorkflowRegistry())
        return asyncio.run(workflow.run_many(items, self.open_store(), **options))

    def keys_for(self, label):
        return [key for recorded_label, key in provider.keys if recorded_label == label]


class StableKeyTest(IdempotencyTestCase):
    def test_key_is_a_uuid(self):
        self.run_workflow(bill, ["cus_1"])
        (key,) = self.keys_for("charge:cus_1")
        self.assertEqual(str(uuid.UUID(key)), key)

    def test_retries_of_one_step_send_the_same_key(self):
        provider.failures = [TransientError("timeout"), TransientError("timeout")]
        statistics = self.run_workflow(bill, ["cus_1"])
        keys = self.keys_for("charge:cus_1")
        self.assertEqual(statistics.items_done, 1)
        self.assertEqual(len(keys), 3)
        self.assertEqual(len(set(keys)), 1)

    def test_rerun_after_a_failure_sends_the_same_key(self):
        provider.failures = [PermanentError("connection lost after the charge")]
        first = self.run_workflow(bill, ["cus_1"])
        second = self.run_workflow(bill, ["cus_1"])
        keys = self.keys_for("charge:cus_1")
        self.assertEqual((first.items_failed, second.items_done), (1, 1))
        self.assertEqual(len(keys), 2)
        self.assertEqual(keys[0], keys[1])

    def test_keys_differ_by_item_step_and_run(self):
        self.run_workflow(charge_then_refund, ["cus_1", "cus_2"])
        self.run_workflow(bill, ["cus_1"], batch_label="2026-12")
        keys = [key for _, key in provider.keys]
        self.assertEqual(len(keys), 5)
        self.assertEqual(len(set(keys)), 5)

    def test_second_call_of_a_step_in_one_item_gets_its_own_key(self):
        self.run_workflow(bill_twice, ["cus_1"])
        keys = self.keys_for("charge:cus_1")
        self.assertEqual(len(keys), 2)
        self.assertNotEqual(keys[0], keys[1])

    def test_scope_gives_one_step_several_keys(self):
        self.run_workflow(scoped, ["cus_1"])
        store = self.open_store()
        ((item_key, _),) = store.items.list_statuses("billing:default")
        result = store.steps.get("billing:default", item_key, "two_requests").result
        self.assertNotEqual(result["create"], result["capture"])


class OutsideAWorkflowTest(unittest.TestCase):
    def test_raises_outside_a_step(self):
        with self.assertRaises(RuntimeError):
            idempotency_key()

    def test_raises_when_a_step_is_called_without_a_workflow(self):
        with self.assertRaises(RuntimeError):
            asyncio.run(charge("cus_1"))


if __name__ == "__main__":
    unittest.main()
