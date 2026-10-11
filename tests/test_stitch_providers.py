import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from stitch import (
    BreakerTuning,
    CheckpointStore,
    NoProviderAvailable,
    ProviderDown,
    ProviderPool,
    QuotaExhausted,
    RateLimited,
    TraceRenderer,
    Workflow,
    WorkflowRecovery,
    WorkflowRegistry,
    step,
)

TUNING = BreakerTuning(
    failure_threshold=2,
    base_cooldown_seconds=0.2,
    maximum_cooldown_seconds=0.4,
    quota_cooldown_seconds=0.3,
    rate_limit_cooldown_seconds=0.1,
)


class ControllableProvider:
    def __init__(self, name, config):
        self.name = name
        self.calls = 0
        self.mode = "ok"

    async def complete(self, text):
        self.calls += 1
        if self.mode == "quota":
            raise QuotaExhausted("insufficient credits")
        if self.mode == "down":
            raise ProviderDown("region unavailable")
        if self.mode == "limited":
            raise RateLimited("slow down", retry_after=0.1)
        return f"{self.name}:{text}"


class ProviderTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.store = CheckpointStore(Path(self.temporary_directory.name) / "checkpoints.db")
        self.providers = {}
        self.pool = ProviderPool(
            "llm",
            self.build_provider,
            default_providers=[("primary", 1, {}), ("secondary", 2, {})],
            tuning=TUNING,
        )
        self.call_counter = 0

    def tearDown(self):
        self.store.close()
        self.temporary_directory.cleanup()

    def build_provider(self, name, config):
        return self.providers.setdefault(name, ControllableProvider(name, config))

    def call_pool(self, text="x"):
        outcome = {}

        async def handler(item_key):
            try:
                outcome["result"] = await self.pool.call(lambda provider: provider.complete(text))
            except Exception as error:
                outcome["error"] = error

        self.call_counter += 1
        workflow = Workflow(handler, name="pool-test", key_function=str, registry=WorkflowRegistry())
        asyncio.run(workflow.run_many([str(self.call_counter)], self.store))
        if "error" in outcome:
            raise outcome["error"]
        return outcome["result"]

    def break_provider(self, name, mode):
        self.build_provider(name, {}).mode = mode


class FailoverTest(ProviderTestCase):
    def test_uses_the_primary_when_healthy(self):
        self.assertEqual(self.call_pool(), "primary:x")

    def test_exhausted_credits_fail_over_and_open_the_breaker(self):
        self.break_provider("primary", "quota")
        self.assertEqual(self.call_pool(), "secondary:x")
        self.assertEqual(self.store.breakers.get("llm", "primary").state, "open")
        calls_before = self.providers["primary"].calls
        self.assertEqual(self.call_pool(), "secondary:x")
        self.assertEqual(self.providers["primary"].calls, calls_before)

    def test_breaker_probes_after_cooldown_and_closes(self):
        self.break_provider("primary", "quota")
        self.call_pool()
        self.providers["primary"].mode = "ok"
        time.sleep(0.35)
        self.assertEqual(self.call_pool(), "primary:x")
        self.assertEqual(self.store.breakers.get("llm", "primary").state, "closed")

    def test_failed_probe_reopens_with_a_longer_cooldown(self):
        self.break_provider("primary", "down")
        for _ in range(TUNING.failure_threshold):
            self.call_pool()
        first_opening = self.store.breakers.get("llm", "primary")
        self.assertEqual(first_opening.state, "open")
        time.sleep(0.25)
        self.call_pool()
        second_opening = self.store.breakers.get("llm", "primary")
        self.assertEqual(second_opening.consecutive_opens, first_opening.consecutive_opens + 1)
        self.assertGreater(second_opening.retry_at - time.time(), 0.25)

    def test_no_provider_available_reports_when_to_retry(self):
        self.break_provider("primary", "quota")
        self.break_provider("secondary", "quota")
        with self.assertRaises(NoProviderAvailable) as context:
            self.call_pool()
        self.assertGreater(context.exception.retry_at, time.time())

    def test_registering_a_provider_at_runtime(self):
        self.break_provider("primary", "quota")
        self.break_provider("secondary", "quota")
        with self.assertRaises(NoProviderAvailable):
            self.call_pool()
        self.pool.register(self.store, "us-west", 2, {"region": "us-west"})
        self.assertEqual(self.call_pool(), "us-west:x")

    def test_rate_limit_is_a_short_cooldown(self):
        self.break_provider("primary", "limited")
        self.assertEqual(self.call_pool(), "secondary:x")
        self.providers["primary"].mode = "ok"
        time.sleep(0.15)
        self.assertEqual(self.call_pool(), "primary:x")


class WorkflowRecoveryTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.store = CheckpointStore(Path(self.temporary_directory.name) / "checkpoints.db")
        self.registry = WorkflowRegistry()
        self.llm_healthy = False
        self.broken_step = False
        self.published = []
        self.pool = ProviderPool(
            "llm", ControllableProvider, default_providers=[("only", 1, {})], tuning=TUNING
        )
        self.triage = self.build_triage_workflow()

    def tearDown(self):
        self.store.close()
        self.temporary_directory.cleanup()

    def build_triage_workflow(self):
        test = self

        async def ask_llm(provider, text):
            if test.broken_step:
                raise ValueError("a bug in the step")
            if not test.llm_healthy:
                raise QuotaExhausted("insufficient credits")
            return await provider.complete(text)

        @step(degrade=lambda event: {"summary": "rule-based", "source": "degraded"})
        async def summarise(event):
            text = await test.pool.call(lambda provider: ask_llm(provider, event["id"]))
            return {"summary": text, "source": "llm"}

        @step
        async def publish(event, summary):
            test.published.append((event["id"], summary["source"]))
            return {"ok": True}

        async def triage(event):
            summary = await summarise(event)
            await publish(event, summary)
            return summary["source"]

        return Workflow(triage, name="triage-test", registry=self.registry)

    def run_events(self, event_ids):
        events = [{"id": event_id} for event_id in event_ids]
        return asyncio.run(self.triage.run_many(events, self.store))

    def heal(self):
        return asyncio.run(WorkflowRecovery(self.store, self.registry).heal())

    def run_id(self):
        return "triage-test:default"


class DegradeAndHealTest(WorkflowRecoveryTestCase):
    def test_degraded_results_are_published_then_replaced_by_heal(self):
        statistics = self.run_events(["e1", "e2"])
        self.assertEqual((statistics.items_done, statistics.items_degraded, statistics.steps_degraded), (2, 2, 2))
        self.assertEqual(self.published, [("e1", "degraded"), ("e2", "degraded")])
        self.llm_healthy = True
        time.sleep(0.35)
        (statistics,) = self.heal().values()
        self.assertEqual((statistics.items_done, statistics.items_degraded), (2, 0))
        self.assertEqual(self.published[2:], [("e1", "llm"), ("e2", "llm")])
        self.assertEqual(self.store.items.list_degraded(self.run_id()), [])

    def test_a_failed_heal_stays_healable(self):
        self.run_events(["e1"])
        self.assertEqual(len(self.store.items.list_degraded(self.run_id())), 1)
        self.broken_step = True
        time.sleep(0.35)
        self.heal()
        self.assertEqual(self.store.items.get(self.run_id(), "e1").status, "failed")
        self.assertEqual(len(self.store.items.list_degraded(self.run_id())), 1)
        self.broken_step, self.llm_healthy = False, True
        self.heal()
        self.assertEqual(self.store.items.get(self.run_id(), "e1").status, "done")
        self.assertEqual(self.store.items.list_degraded(self.run_id()), [])
        self.assertEqual(self.published[-1], ("e1", "llm"))

    def test_audit_trail_tells_the_story(self):
        self.run_events(["q"])
        records = self.store.audit.list_records(item_key="q")
        events = [record.event for record in records]
        for expected in ("item_started", "step_started", "provider_failed", "breaker_opened", "step_degraded", "step_done", "item_done"):
            self.assertIn(expected, events)
        text = TraceRenderer().render(records)
        self.assertIn("breaker OPEN", text)
        self.assertIn("used its fallback", text)


class ParkAndResumeTest(WorkflowRecoveryTestCase):
    def test_item_without_fallback_parks_and_retries_after_the_cooldown(self):
        @step
        async def strict_step(event):
            return await self.pool.call(lambda provider: ask(provider, event["id"]))

        recovered = {"value": False}

        async def ask(provider, text):
            if not recovered["value"]:
                raise QuotaExhausted("insufficient credits")
            return await provider.complete(text)

        async def strict_handler(event):
            return await strict_step(event)

        strict_workflow = Workflow(strict_handler, name="strict-test", registry=self.registry)

        async def scenario():
            async def recover_later():
                await asyncio.sleep(0.2)
                recovered["value"] = True

            recovery = asyncio.create_task(recover_later())
            statistics = await strict_workflow.run_many([{"id": "a"}], self.store, wait_for_parked=True)
            await recovery
            return statistics

        statistics = asyncio.run(scenario())
        self.assertEqual((statistics.items_parked, statistics.items_done, statistics.parked_remaining), (1, 1, 0))
        events = [record.event for record in self.store.audit.list_records(item_key="a")]
        self.assertIn("item_parked", events)
        self.assertIn("breaker_opened", events)
        self.assertIn("cooldown_over", [record.event for record in self.store.audit.list_records()])

    def test_resume_restarts_unfinished_items_from_their_stored_input(self):
        self.llm_healthy = True
        run_id = self.run_id()
        self.store.runs.open_run(run_id, "triage-test", {})
        self.store.items.mark_running(run_id, "e9", '{"id": "e9"}')
        self.store.steps.mark_started(run_id, "e9", "summarise", 1)
        self.store.steps.mark_done(run_id, "e9", "summarise", {"summary": "kept", "source": "llm"}, 1, False)
        outcomes = asyncio.run(WorkflowRecovery(self.store, self.registry).resume())
        (statistics,) = outcomes.values()
        self.assertEqual((statistics.items_done, statistics.steps_skipped, statistics.steps_run), (1, 1, 1))
        self.assertEqual(self.published, [("e9", "llm")])


if __name__ == "__main__":
    unittest.main()
