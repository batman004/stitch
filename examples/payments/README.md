# Payments example: charge once, even after `kill -9`

A monthly billing job charges 10 customers through a payments API. Halfway through, the process dies: a deploy, the OOM killer, someone pressing Ctrl+C. You run it again.

With a plain `for` loop, everyone charged before the crash is charged a second time. This example shows how stitch prevents that, and how to handle the payment provider's webhooks so each receipt goes out exactly once.

It runs entirely on your machine. `fake_payments_api.py` is a small local stand-in that behaves like a Mollie-style payments API, so there is no account, no API key and no real money involved.

## The problem in one picture

```text
billing.py ──POST /v2/payments──▶ payments API
                                   creates the payment ✓
           ◀──── 201 tr_… ──────── (reply on its way)
   💀 process killed here
   nothing was saved, so the rerun charges this customer again
```

Two things fix it together:

1. **Checkpoints.** Each step's result is written to SQLite as it finishes. A rerun skips customers that are done.
2. **A stable `Idempotency-Key`.** A crash can still land after the provider said yes but before the checkpoint was written. `idempotency_key()` sends the same key on the rerun, so the provider returns the first payment instead of creating a new one.

## Run it

Python 3.11+, no dependencies. From this folder, in three terminals:

```bash
export PYTHONPATH=../..                # use the stitch in this repo

python fake_payments_api.py            # terminal 1: the local payments API on :8765
python billing.py webhooks             # terminal 2: webhook receiver on :8766 and its worker
python billing.py                      # terminal 3: charge everyone
```

`python billing.py --reset` clears the client's state and the API's payments. Restart terminal 2 after a reset.

`python billing.py report` checks the payments at any time, without charging anyone: one line per payment (customer, payment id, status, receipt), then two summaries. The first says who was charged twice and who was not charged. The second says how many receipts went out, which are still pending, and which never arrived. Use it right after a kill, before you rerun, to see the half-finished state.

`python billing.py reconcile` recovers receipts for webhooks that never arrived (see below).

## Try this

Each run ends with the same `report` summary, counted from the API's own payment list, so you can see whether anyone paid twice.

| Run | How to break it | Result |
|---|---|---|
| `python billing.py --plain` | Kill it after a few customers, run it again | Everyone charged before the kill is charged twice |
| `python billing.py --random-keys` | Kill it right after terminal 1 logs `payment created` | Finished customers are skipped; the one in flight is charged twice |
| `python billing.py`, kill it the same way, then `python billing.py --new-price` | Same crash, but the rerun asks for 10.99 instead of 9.99 | The key is reused with a different body: the API logs `idempotency_key_mismatch`, answers 400, and that customer fails instead of being charged again |
| `python billing.py` | Same kill | Nobody is charged twice; the API replays the first payment |

The API waits 2 seconds between creating a payment and replying. Kill the client inside that window to hit the worst case: the payment exists, but your code never heard about it.

Also try:

- **Stop and restart the API** mid-run. The client retries the connection and carries on.
- **Kill the webhook receiver** (terminal 2) while billing runs. The API retries each delivery 5 times, 2 seconds apart, then logs `webhook given up` and stops. The charge itself was fine, but the receipt is now lost: `python billing.py report` shows `receipts … not_received=cus_1,…` and `action=run_reconcile`.
- **Recover it:** start the receiver again, then run `python billing.py reconcile`. It lists the payments from the API and records every paid one in the inbox. Receipts already handled are ignored as duplicates, so only the missing ones go out, and running it twice changes nothing.

## What the stand-in API does on purpose

| Behaviour | Why it is there |
|---|---|
| Same `Idempotency-Key` and same body: returns the first response with `Idempotent-Replayed: true` | How providers recognise a repeated request |
| Same key with a different body: `400` | The rerun must build exactly the same request |
| Every 5th create: `429` with `Retry-After: 1` | Shows retries that wait as long as the server asks |
| `cus_6` has amount `0.00`: `422` | One bad request fails one customer, not the run. `report` always lists `cus_6` under `not_charged` |
| Payment marked `paid` 3 s later, webhook `id=tr_…` sent twice | Webhooks arrive more than once |
| A failed webhook is retried 5 times, 2 s apart, then dropped | Providers stop retrying eventually (Mollie: 10 times over about 26 hours), so something must catch up: `reconcile` |
| State saved to `payments.json` | Restarting the API keeps its payments |

It is not Mollie and does not talk to Mollie. It follows the documented behaviour of these endpoints closely enough to show the client side.

## How stitch is used

The charge is a step. Its result is checkpointed, temporary errors are retried, and the key stays the same across retries and reruns:

```python
@step(retry_policy=RetryPolicy(maximum_attempts=6, retry_on=(OSError, TransientError)))
async def charge(customer):
    return await create_payment(customer, idempotency_key())

billing = Workflow(bill, name="billing", key_function=lambda c: c["id"])
await billing.run_many(CUSTOMERS, CheckpointStore("state.db"))
```

The HTTP client maps responses to stitch errors: `409`, `429` and `5xx` become `TransientError` (retried, honouring `Retry-After`); any other `4xx` is a `PermanentError` (that customer fails, the run continues).

Webhooks can also be lost, so `reconcile` runs the same `accept()` for every paid payment the API lists. It is the nightly job from the talk, as one command.

Webhooks use the inbox: the handler fetches the payment, records `payment:status` with `accept()` in one short write, and answers `200` at once. A worker runs `process_pending()` to send the receipt. A repeated delivery has the same key, so it is ignored:

```python
payment_events = Workflow(handle_event, name="payment-events",
                          key_function=lambda e: f"{e['payment']}:{e['status']}")

payment_events.accept(event, store)          # in the webhook handler
await payment_events.process_pending(store)  # in one worker loop
```

## Reading the logs

Both programs log to stderr with the standard `logging` module, as `key=value` pairs. Set `LOG_LEVEL=DEBUG` for skipped steps and HTTP access lines.

```text
WARNING billing step retry item=cus_5 step=charge attempt=1 error='429 Slow down.' delay=1.0s
ERROR   billing item failed item=cus_6 error='charge: 422 The amount is invalid.'
WARNING billing step resumed item=cus_8 step=charge reason=running_when_process_stopped
INFO    billing step done item=cus_8 step=charge payment=tr_rWMTqemQ replayed=true
INFO    billing report payments=9 customers=9 charged_twice=none not_charged=cus_6 statuses=paid:9
WARNING billing report receipts paid=9 sent=0 pending=none not_received=cus_1,cus_2,cus_3,…
WARNING billing report action=run_reconcile reason=webhook_never_received customers=9
WARNING billing reconcile recovered customer=cus_1 payment=tr_-oRGo83p status=paid
INFO    billing reconcile finished recovered=9 already_known=0
INFO    billing.webhooks webhook duplicate ignored payment=tr_M1kRWFxe status=paid
```

## Files

| File | What it is |
|---|---|
| `billing.py` | The client: the billing workflow, the plain-loop comparison and the webhook receiver |
| `fake_payments_api.py` | The local payments API stand-in |
| `state.db`, `events.db`, `payments.json` | Created when you run it; delete them or use `--reset` to start over |

The same pattern works for any job that makes calls you can't afford to repeat: API syncs, LLM pipelines, document ingestion. See the main [README](../../README.md) for those use cases.
