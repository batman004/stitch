# stitch

Resumable async Python workflows from the standard library alone: checkpoints, retries, a flagged fallback when a
dependency is down, and a record of every step.

```bash
pip install -e .
```

## In one screen

```python
from stitch import CheckpointStore, RetryPolicy, Workflow, step

@step(retry_policy=RetryPolicy(retry_on=(OSError,)))
async def download(article): ...

@step(degrade=quick_fallback)
async def analyse(article, text): ...

async def process(article):
    text = await download(article)
    await analyse(article, text)

store = CheckpointStore("state.db")
await Workflow(process, name="demo").run_many(articles, store)
```

Every completed step is saved before the next one starts, so a rerun skips what is done. A step is retried with
backoff; when retries run out, `degrade` supplies a flagged fallback instead of stopping the run, and
`WorkflowRecovery(store).heal()` redoes those steps later. Steps must be safe to run twice, because a crash can land
after a step's side effect but before its checkpoint is written. A step marked `critical` (`@step(critical=CriticalPolicy(health_check=...))`) halts the whole run when it fails for
good: stitch sends an `Alerter` alert once, raises `CriticalStepFailed`, and refuses to start that run again until the
health check passes. A critical step cannot also `degrade`. A workflow takes one JSON-serialisable argument and
one process drives one state file.

## Tests

```bash
python -m unittest
```

No dependencies, so no virtual environment is needed to run them. MIT licence.
