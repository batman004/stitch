import asyncio
import unittest

from stitch import (
    Alerter,
    CriticalPolicy,
    CriticalStepFailed,
    HealthCheckFailed,
    PermanentError,
    RetryPolicy,
    TransientError,
    step,
)
from tests.test_stitch_engine import FAST_RETRY, EngineTestCase

RUN_ID = "engine-test:default"


class FakeResource:
    def __init__(self):
        self.down = False
        self.stored = []
        self.gathered = []

    def store(self, item_key):
        if self.down:
            raise TransientError("resource down")
        self.stored.append(item_key)

    def check(self):
        if self.down:
            raise ConnectionError("still down")
        return True


class RecordingAlerter(Alerter):
    def __init__(self):
        self.alerts = []

    def send(self, alert):
        self.alerts.append(alert)


class BrokenAlerter(Alerter):
    def send(self, alert):
        raise RuntimeError("smtp is down")


resource = FakeResource()


@step(retry_policy=FAST_RETRY)
async def gather_step(item_key):
    resource.gathered.append(item_key)
    return item_key


@step(retry_policy=FAST_RETRY, critical=CriticalPolicy(health_check=resource.check))
async def checked_store_step(item_key):
    resource.store(item_key)


@step(retry_policy=FAST_RETRY, critical=True)
async def unchecked_store_step(item_key):
    resource.store(item_key)


@step(retry_policy=RetryPolicy(maximum_attempts=1), critical=True)
async def strict_step(item_key):
    raise PermanentError("bad data")


async def checked_handler(item_key):
    await gather_step(item_key)
    await checked_store_step(item_key)


async def unchecked_handler(item_key):
    await gather_step(item_key)
    await unchecked_store_step(item_key)


async def strict_handler(item_key):
    await gather_step(item_key)
    await strict_step(item_key)


class CriticalStepTest(EngineTestCase):
    def setUp(self):
        super().setUp()
        resource.down = False
        resource.stored.clear()
        resource.gathered.clear()
        self.alerter = RecordingAlerter()

    def run_critical(self, items, handler=checked_handler, alerter=None):
        workflow = self.build_workflow(handler)
        return asyncio.run(workflow.run_many(items, self.open_store(), concurrency=3, alerter=alerter or self.alerter))

    def halt_the_run(self):
        resource.down = True
        with self.assertRaises(CriticalStepFailed):
            self.run_critical(["a", "b", "c"])

    def test_a_critical_step_that_runs_out_of_retries_halts_the_whole_run_and_alerts_once(self):
        self.halt_the_run()
        self.assertEqual([alert.severity for alert in self.alerter.alerts], ["critical"])
        self.assertEqual(self.alerter.alerts[0].step_name, "checked_store_step")
        store = self.open_store()
        self.assertEqual(store.runs.get_halt(RUN_ID)[0], "checked_store_step")
        self.assertNotIn("failed", [status for _, status in store.items.list_statuses(RUN_ID)])

    def test_a_halted_run_stays_blocked_while_the_health_check_fails(self):
        self.halt_the_run()
        resource.gathered.clear()
        with self.assertRaises(HealthCheckFailed):
            self.run_critical(["a", "b", "c"])
        self.assertEqual(resource.gathered, [])
        self.assertEqual(len(self.alerter.alerts), 1)
        self.assertIsNotNone(self.open_store().runs.get_halt(RUN_ID))

    def test_the_run_resumes_when_the_resource_is_healthy_and_does_not_repeat_finished_steps(self):
        self.halt_the_run()
        gathered_before = sorted(set(resource.gathered))
        resource.down = False
        resource.gathered.clear()
        statistics = self.run_critical(["a", "b", "c"])
        self.assertEqual(statistics.items_done, 3)
        self.assertEqual(sorted(resource.stored), ["a", "b", "c"])
        self.assertEqual(resource.gathered, [])
        self.assertEqual(gathered_before, ["a", "b", "c"])
        self.assertEqual([alert.severity for alert in self.alerter.alerts], ["critical", "recovered"])
        self.assertIsNone(self.open_store().runs.get_halt(RUN_ID))

    def test_a_critical_step_without_a_health_check_may_be_run_again_straight_away(self):
        resource.down = True
        with self.assertRaises(CriticalStepFailed):
            self.run_critical(["a"], handler=unchecked_handler)
        resource.down = False
        statistics = self.run_critical(["a"], handler=unchecked_handler)
        self.assertEqual(statistics.items_done, 1)

    def test_a_permanent_error_in_a_critical_step_fails_the_item_but_does_not_halt(self):
        statistics = self.run_critical(["a", "b"], handler=strict_handler)
        self.assertEqual(statistics.items_failed, 2)
        self.assertEqual(self.alerter.alerts, [])

    def test_a_broken_alerter_does_not_hide_the_halt(self):
        resource.down = True
        with self.assertRaises(CriticalStepFailed):
            self.run_critical(["a"], alerter=BrokenAlerter())
        events = [record.event for record in self.open_store().audit.list_records()]
        self.assertIn("alert_failed", events)

    def test_a_critical_step_cannot_also_degrade(self):
        with self.assertRaises(ValueError):
            step(critical=True, degrade=lambda item_key: None)(lambda item_key: None)


if __name__ == "__main__":
    unittest.main()
