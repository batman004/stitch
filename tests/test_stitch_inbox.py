import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from stitch_durable import (
    CheckpointStore,
    Park,
    Workflow,
    WorkflowRecovery,
    WorkflowRegistry,
    step,
)


class SimulatedCrash(BaseException):
    """Escapes every ``except Exception``, like a process dying mid-step."""


class Calls:
    def __init__(self):
        self.made = []
        self.crash_on = None
        self.park_on = None

    def record(self, step_name, event_id):
        self.made.append((step_name, event_id))
        if self.crash_on == (step_name, event_id):
            self.crash_on = None
            raise SimulatedCrash()
        if self.park_on == (step_name, event_id):
            raise Park(self.park_until, "provider cooling down")


calls = Calls()


@step
async def mark_order_paid(event):
    calls.record("mark_paid", event["id"])
    return {"order": event["order"]}


@step
async def send_receipt(event):
    calls.record("send_receipt", event["id"])
    return True


async def handle_paid(event):
    await mark_order_paid(event)
    return await send_receipt(event)


def event_key(event):
    return f"{event['payment']}:{event['status']}"


def paid(payment, order="ord_1", status="paid"):
    return {
        "id": f"{payment}:{status}",
        "payment": payment,
        "order": order,
        "status": status,
    }


class InboxTestCase(unittest.TestCase):
    def setUp(self):
        calls.made.clear()
        calls.crash_on = None
        calls.park_on = None
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary_directory.name) / "state.db"
        self.registry = WorkflowRegistry()
        self.workflow = Workflow(
            handle_paid, name="webhooks", key_function=event_key, registry=self.registry
        )
        self.store = CheckpointStore(self.path)

    def tearDown(self):
        self.store.close()
        self.temporary_directory.cleanup()

    def status_of(self, item_key):
        return self.store.items.get("webhooks:default", item_key).status

    def process(self):
        return asyncio.run(self.workflow.process_pending(self.store))


class AcceptTest(InboxTestCase):
    def test_accept_records_the_event_without_running_it(self):
        self.assertTrue(self.workflow.accept(paid("pay_1"), self.store))
        self.assertEqual(calls.made, [])
        self.assertEqual(self.status_of("pay_1:paid"), "pending")

    def test_a_repeated_delivery_is_recorded_once(self):
        self.assertTrue(self.workflow.accept(paid("pay_1"), self.store))
        self.assertFalse(self.workflow.accept(paid("pay_1"), self.store))
        self.assertEqual(len(self.store.items.list_statuses("webhooks:default")), 1)

    def test_a_new_status_of_the_same_payment_is_a_new_item(self):
        self.workflow.accept(paid("pay_1"), self.store)
        self.assertTrue(
            self.workflow.accept(paid("pay_1", status="refunded"), self.store)
        )

    def test_accept_rejects_an_item_that_is_not_json(self):
        with self.assertRaises(TypeError):
            self.workflow.accept(
                {"payment": "pay_1", "status": "paid", "at": object()}, self.store
            )

    def test_accept_writes_the_audit_trail(self):
        self.workflow.accept(paid("pay_1"), self.store)
        self.workflow.accept(paid("pay_1"), self.store)
        events = [
            record.event
            for record in self.store.audit.list_records(item_key="pay_1:paid")
        ]
        self.assertEqual(events, ["item_accepted", "item_duplicate"])


class ProcessPendingTest(InboxTestCase):
    def test_pending_events_are_processed_once(self):
        self.workflow.accept(paid("pay_1"), self.store)
        self.workflow.accept(paid("pay_2", order="ord_2"), self.store)
        first = self.process()
        second = self.process()
        self.assertEqual(first.items_done, 2)
        self.assertEqual(second.items_done + second.steps_run, 0)
        self.assertEqual(len(calls.made), 4)

    def test_a_delivery_after_processing_is_not_handled_again(self):
        self.workflow.accept(paid("pay_1"), self.store)
        self.process()
        self.assertFalse(self.workflow.accept(paid("pay_1"), self.store))
        self.process()
        self.assertEqual(
            calls.made, [("mark_paid", "pay_1:paid"), ("send_receipt", "pay_1:paid")]
        )

    def test_nothing_waiting_opens_no_run(self):
        statistics = self.process()
        self.assertEqual(statistics.items_done, 0)
        self.assertEqual(self.store.runs.list_runs(), [])

    def test_a_crash_mid_event_resumes_from_the_finished_step(self):
        self.workflow.accept(paid("pay_1"), self.store)
        calls.crash_on = ("send_receipt", "pay_1:paid")
        with self.assertRaises(BaseException):
            self.process()
        self.assertEqual(self.status_of("pay_1:paid"), "running")
        statistics = self.process()
        self.assertEqual(statistics.items_done, 1)
        self.assertEqual(statistics.steps_skipped, 1)
        self.assertEqual(
            calls.made,
            [
                ("mark_paid", "pay_1:paid"),
                ("send_receipt", "pay_1:paid"),
                ("send_receipt", "pay_1:paid"),
            ],
        )

    def test_a_parked_event_waits_until_it_is_due(self):
        self.workflow.accept(paid("pay_1"), self.store)
        calls.park_on = ("mark_paid", "pay_1:paid")
        calls.park_until = time.time() + 60
        self.process()
        self.assertEqual(self.status_of("pay_1:paid"), "parked")
        calls.made.clear()
        self.process()
        self.assertEqual(calls.made, [])
        calls.park_on = None
        self.store.items.mark_parked(
            "webhooks:default", "pay_1:paid", time.time() - 1, "due"
        )
        self.assertEqual(self.process().items_done, 1)

    def test_resume_also_finds_accepted_events(self):
        self.workflow.accept(paid("pay_1"), self.store)
        outcomes = asyncio.run(WorkflowRecovery(self.store, self.registry).resume())
        self.assertEqual(outcomes["webhooks:default"].items_done, 1)

    def test_new_work_reopens_a_finished_run(self):
        self.workflow.accept(paid("pay_1"), self.store)
        self.process()
        self.assertEqual(self.store.runs.list_runs()[0][2], "done")
        self.workflow.accept(paid("pay_2"), self.store)
        self.assertEqual(self.store.runs.list_runs()[0][2], "pending")


if __name__ == "__main__":
    unittest.main()
