import asyncio
import tempfile
import unittest
from pathlib import Path

from stitch import (
    CheckpointStore,
    PermanentError,
    RetryPolicy,
    TransientError,
    Workflow,
    WorkflowRegistry,
    step,
)

FAST_RETRY = RetryPolicy(maximum_attempts=3, base_delay_seconds=0.001, maximum_delay_seconds=0.001)


class CallRecorder:
    def __init__(self):
        self.calls = []
        self.failures = {}

    def record(self, step_name, item_key):
        self.calls.append((step_name, item_key))
        pending_failures = self.failures.get((step_name, item_key))
        if pending_failures:
            raise pending_failures.pop(0)


recorder = CallRecorder()


@step(retry_policy=FAST_RETRY)
async def first_step(item_key):
    recorder.record("first", item_key)
    return {"first": item_key}


@step(retry_policy=FAST_RETRY)
async def second_step(item_key):
    recorder.record("second", item_key)
    return "second-result"


@step(retry_policy=RetryPolicy(maximum_attempts=1))
async def fragile_second_step(item_key):
    recorder.record("second", item_key)
    return "second-result"


async def two_step_handler(item_key):
    await first_step(item_key)
    return await second_step(item_key)


async def fragile_handler(item_key):
    await first_step(item_key)
    return await fragile_second_step(item_key)


class EngineTestCase(unittest.TestCase):
    def setUp(self):
        recorder.calls.clear()
        recorder.failures.clear()
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "checkpoints.db"
        self.stores = []

    def tearDown(self):
        for store in self.stores:
            store.close()
        self.temporary_directory.cleanup()

    def open_store(self):
        store = CheckpointStore(self.database_path)
        self.stores.append(store)
        return store

    def build_workflow(self, handler=two_step_handler, **options):
        return Workflow(handler, name="engine-test", key_function=str, registry=WorkflowRegistry(), **options)

    def run_items(self, items, handler=two_step_handler, **options):
        workflow = self.build_workflow(handler, **options.pop("workflow_options", {}))
        return asyncio.run(workflow.run_many(items, self.open_store(), **options))


class ResumeTest(EngineTestCase):
    def test_resume_skips_completed_steps(self):
        recorder.failures[("second", "b")] = [PermanentError("boom")]
        statistics = self.run_items(["a", "b"])
        self.assertEqual(statistics.items_failed, 1)
        recorder.calls.clear()
        statistics = self.run_items(["a", "b"])
        self.assertEqual(recorder.calls, [("second", "b")])
        self.assertEqual(
            (statistics.items_skipped, statistics.items_done, statistics.steps_skipped), (1, 1, 1)
        )

    def test_finished_run_does_nothing_again(self):
        self.run_items(["a", "b"])
        recorder.calls.clear()
        statistics = self.run_items(["a", "b"])
        self.assertEqual(recorder.calls, [])
        self.assertEqual(statistics.items_skipped, 2)

    def test_second_call_returns_the_stored_result_without_running_again(self):
        self.run_items(["a"])
        store = self.open_store()
        record = store.items.get("engine-test:default", "a")
        self.assertEqual((record.status, record.result), ("done", "second-result"))

    def test_repeated_step_in_one_item_gets_distinct_ids(self):
        async def handler(item_key):
            await first_step(item_key)
            await first_step(item_key)

        self.run_items(["a"], handler=handler)
        step_names = [record.step_name for record in self.open_store().steps.list_for_run("engine-test:default")]
        self.assertEqual(step_names, ["first_step", "first_step#2"])

    def test_concurrent_run_processes_every_item(self):
        keys = [str(number) for number in range(50)]
        statistics = self.run_items(keys, concurrency=8)
        self.assertEqual(statistics.items_done, 50)
        self.assertEqual(len(recorder.calls), 100)


class RetryTest(EngineTestCase):
    def test_step_retries_transient_errors_and_only_that_step_repeats(self):
        recorder.failures[("second", "a")] = [TransientError("503"), TransientError("503")]
        statistics = self.run_items(["a"])
        self.assertEqual(statistics.step_retries, 2)
        self.assertEqual(recorder.calls.count(("first", "a")), 1)
        self.assertEqual(recorder.calls.count(("second", "a")), 3)

    def test_permanent_error_fails_the_item_and_others_continue(self):
        recorder.failures[("first", "a")] = [PermanentError("bad input")]
        statistics = self.run_items(["a", "b"])
        self.assertEqual((statistics.items_failed, statistics.items_done), (1, 1))

    def test_item_retry_restarts_the_item_but_reuses_checkpoints(self):
        recorder.failures[("second", "a")] = [TransientError("503")]
        statistics = self.run_items(
            ["a"], handler=fragile_handler, workflow_options={"item_retry_policy": FAST_RETRY}
        )
        self.assertEqual(statistics.item_retries, 1)
        self.assertEqual(statistics.items_done, 1)
        self.assertEqual(recorder.calls.count(("first", "a")), 1)
        self.assertEqual(recorder.calls.count(("second", "a")), 2)


if __name__ == "__main__":
    unittest.main()
