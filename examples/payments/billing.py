"""Charge a list of customers through a payments API, and survive failures.

    python fake_payments_api.py       # terminal 1: the local API
    python billing.py                 # terminal 2: charge everyone, with stitch
    python billing.py --plain         # the same calls in a bare for loop, no stitch
    python billing.py report          # check the payments now: charged twice? receipts sent?
    python billing.py reconcile       # recover receipts for webhooks that never arrived

Try, by hand, while it runs:
  * Ctrl+C, or kill -9 the script during a charge, then run it again: finished work is skipped.
  * kill -9 right after the API logs "created" (it waits 2s before replying): the payment exists but
    stitch never saved it. Rerun: the same Idempotency-Key makes the API replay it. No double charge.
    Compare with `python billing.py --random-keys`, which charges that customer twice.
  * Stop the API and restart it: the client retries, then picks up on the next run.
  * `python billing.py --reset` starts over.

Webhooks: run `python billing.py webhooks` in a third terminal before billing. A few seconds after each
charge the API marks the payment paid and calls the receiver, twice. The receiver fetches the payment,
records the event with Workflow.accept (a duplicate is ignored), answers 200, and a worker loop runs
process_pending to send the receipt. Kill the receiver and restart it quickly: the API's retries deliver
what it missed. Leave it down longer and the API gives up after about 10 seconds per payment; run
`python billing.py reconcile` to recover those receipts, and `python billing.py report` to see which are missing.

Logs go to stderr in key=value form. Set LOG_LEVEL=DEBUG for HTTP access lines and skipped steps.

What stitch adds to a plain loop: @step checkpoints, retries that honour Retry-After,
a PermanentError that fails one customer without stopping the run, and idempotency_key().
"""

import argparse
import asyncio
import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from stitch_durable import (
    CheckpointStore,
    PermanentError,
    Reporter,
    RetryPolicy,
    TransientError,
    Workflow,
    idempotency_key,
    step,
)

API = "http://127.0.0.1:8765"
STATE = Path("state.db")
EVENTS = Path("events.db")
WEBHOOK_URL = "http://127.0.0.1:8766/webhook"
CUSTOMERS = [{"id": f"cus_{n}", "amount": "9.99"} for n in range(1, 11)]
CUSTOMERS[5]["amount"] = "0.00"  # cus_6: the API will answer 422

args = argparse.Namespace()
log = logging.getLogger("billing")
webhook_log = logging.getLogger("billing.webhooks")


# ---------- the payments client ----------


def call(method, path, body=None, headers=None):
    """One HTTP call. 409, 429 and 5xx become TransientError (stitch retries); other 4xx are PermanentError."""
    request = urllib.request.Request(
        API + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer test_demo",
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response), response.headers
    except urllib.error.HTTPError as error:
        detail = json.load(error).get("detail", error.reason)
        if error.code in (409, 429) or error.code >= 500:
            wait = float(error.headers.get("Retry-After", 0)) or None
            raise TransientError(f"{error.code} {detail}", retry_after=wait) from None
        raise PermanentError(f"{error.code} {detail}") from None


@step(
    retry_policy=RetryPolicy(
        maximum_attempts=6,
        base_delay_seconds=1,
        maximum_delay_seconds=5,
        retry_on=(OSError, TransientError),
    )
)
async def charge(customer):
    # The same key on every retry, rerun and heal. A random one is what many SDKs send by default.
    key = str(uuid.uuid4()) if args.random_keys else idempotency_key()
    return await create_payment(customer, key)


async def create_payment(customer, key):
    payment, headers = await asyncio.to_thread(
        call,
        "POST",
        "/v2/payments",
        {
            "amount": {"currency": "EUR", "value": customer["amount"]},
            "description": f"Subscription {customer['id']}",
            "redirectUrl": "https://example.com/thanks",
            "webhookUrl": WEBHOOK_URL,
        },
        {"Idempotency-Key": key},
    )
    return {
        "payment": payment["id"],
        "replayed": headers.get("Idempotent-Replayed") == "true",
    }


async def run_plain():
    """The loop most of us write first: no checkpoints, no retries, a fresh key per request."""
    for customer in CUSTOMERS:
        try:
            payment = await create_payment(customer, str(uuid.uuid4()))
            log.info(
                "charged customer=%s payment=%s", customer["id"], payment["payment"]
            )
        except Exception as error:
            log.error("charge failed customer=%s error=%r", customer["id"], str(error))


async def bill(customer):
    return await charge(customer)


# ---------- webhooks: accept fast, process later ----------


@step
async def send_receipt(event):
    webhook_log.info(
        "receipt sent customer=%s payment=%s", event["customer"], event["payment"]
    )
    return True


async def handle_event(event):
    if event["status"] == "paid":
        await send_receipt(event)


# one event per payment and status, so a repeated delivery is the same item
payment_events = Workflow(
    handle_event,
    name="payment-events",
    key_function=lambda e: f"{e['payment']}:{e['status']}",
)


def event_from(payment):
    """Our view of a payment: what the inbox stores, one item per payment and status."""
    return {
        "payment": payment["id"],
        "status": payment["status"],
        "customer": payment["description"].split()[-1],
    }


def accept(event):
    """Record an event in the inbox. Returns False when that payment and status was seen before."""
    # a short-lived connection: the HTTP handler runs in another thread
    store = CheckpointStore(EVENTS)
    try:
        return payment_events.accept(event, store)
    finally:
        store.close()


def accept_event(payment_id):
    """Mollie-style webhook: only an id arrives. Fetch the payment, record it, and return fast."""
    payment, _ = call("GET", f"/v2/payments/{payment_id}")
    event = event_from(payment)
    if accept(event):
        webhook_log.info(
            "webhook accepted payment=%s status=%s", payment["id"], event["status"]
        )
    else:
        webhook_log.info(
            "webhook duplicate ignored payment=%s status=%s",
            payment["id"],
            event["status"],
        )


class Receiver(BaseHTTPRequestHandler):
    def do_POST(self):
        fields = urllib.parse.parse_qs(
            self.rfile.read(int(self.headers["Content-Length"])).decode()
        )
        try:
            accept_event(fields["id"][0])
            status = 200
        except Exception:  # answer 500 so the API retries
            webhook_log.exception(
                "webhook failed payment=%s", fields.get("id", ["?"])[0]
            )
            status = 500
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        webhook_log.debug("http %s %s", self.address_string(), format % args)


async def run_webhooks():
    server = ThreadingHTTPServer(("127.0.0.1", 8766), Receiver)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    webhook_log.info("listening url=%s store=%s", WEBHOOK_URL, EVENTS)
    store = CheckpointStore(EVENTS)
    while (
        True
    ):  # the worker: runs whatever was accepted and not finished, including after a crash
        await payment_events.process_pending(store, reporter=LogReporter(webhook_log))
        await asyncio.sleep(0.5)


# ---------- running it ----------


class LogReporter(Reporter):
    """Turns stitch's live events into log records."""

    def __init__(self, logger):
        self.log = logger

    def on_step_started(self, item, step, interrupted):
        if interrupted:
            self.log.warning(
                "step resumed item=%s step=%s reason=running_when_process_stopped",
                item,
                step,
            )

    def on_step_done(self, item, step, result):
        if isinstance(result, dict) and "payment" in result:
            self.log.info(
                "step done item=%s step=%s payment=%s replayed=%s",
                item,
                step,
                result["payment"],
                str(result.get("replayed", False)).lower(),
            )
        else:
            self.log.debug("step done item=%s step=%s", item, step)

    def on_step_skipped(self, item, step):
        self.log.debug("step skipped item=%s step=%s reason=already_done", item, step)

    def on_item_skipped(self, item):
        self.log.debug("item skipped item=%s reason=already_done", item)

    def on_step_retry(self, item, step, attempt, error, delay):
        self.log.warning(
            "step retry item=%s step=%s attempt=%d error=%r delay=%.1fs",
            item,
            step,
            attempt,
            str(error),
            delay,
        )

    def on_item_failed(self, item, error):
        self.log.error("item failed item=%s error=%r", item, str(error))


def receipt_states(payments):
    """For each paid payment: was its webhook ever received, and has its receipt gone out?

    sent         the receipt step finished
    pending      the webhook was received, the worker has not sent the receipt yet
    not_received no webhook arrived (the receiver was down and the API gave up)
    """
    if not EVENTS.exists():
        return {pid: "not_received" for pid in payments}
    store = CheckpointStore(EVENTS)
    try:
        run = payment_events._run_id(None)
        states = {}
        for pid in payments:
            item = store.items.get(run, f"{pid}:paid")
            states[pid] = (
                "not_received"
                if item is None
                else "sent"
                if item.status == "done"
                else "pending"
            )
        return states
    finally:
        store.close()


def report(detail=False):
    """Count payments from the API's own list: who was charged, once or twice, and who got a receipt."""
    try:
        payments, _ = call("GET", "/v2/payments")
    except OSError as error:
        return log.error("report failed reason=api_unreachable error=%r", str(error))
    rows = [
        (p["description"].split()[-1], p["id"], p["status"])
        for p in reversed(payments["_embedded"]["payments"])
    ]
    paid = {payment: customer for customer, payment, status in rows if status == "paid"}
    receipts = receipt_states(paid)
    if detail:
        for customer, payment, status in rows:
            log.info(
                "payment customer=%s payment=%s status=%s receipt=%s",
                customer,
                payment,
                status,
                receipts.get(payment, "-"),
            )
    charged = [customer for customer, _, _ in rows]
    twice = sorted({c for c in charged if charged.count(c) > 1})
    missing = [c["id"] for c in CUSTOMERS if c["id"] not in charged]
    statuses = {s: [r[2] for r in rows].count(s) for s in sorted({r[2] for r in rows})}
    log.log(
        logging.ERROR if twice else logging.INFO,
        "report payments=%d customers=%d charged_twice=%s not_charged=%s statuses=%s",
        len(rows),
        len(set(charged)),
        ",".join(twice) or "none",
        ",".join(missing) or "none",
        ",".join(f"{k}:{v}" for k, v in statuses.items()) or "none",
    )
    by_state = {
        state: sorted(paid[pid] for pid, st in receipts.items() if st == state)
        for state in ("sent", "pending", "not_received")
    }
    log.log(
        (
            logging.WARNING
            if by_state["pending"] or by_state["not_received"]
            else logging.INFO
        ),
        "report receipts paid=%d sent=%d pending=%s not_received=%s",
        len(paid),
        len(by_state["sent"]),
        ",".join(by_state["pending"]) or "none",
        ",".join(by_state["not_received"]) or "none",
    )
    if by_state["not_received"]:
        log.warning(
            "report action=run_reconcile reason=webhook_never_received customers=%d",
            len(by_state["not_received"]),
        )


def reconcile():
    """Catch up on webhooks that never arrived: record every paid payment in the inbox.

    Payments the webhook already delivered are ignored as duplicates, so this is safe to run any time,
    and twice. The receiver's worker (`billing.py webhooks`) sends the receipts that were missing.
    """
    try:
        payments, _ = call("GET", "/v2/payments")
    except OSError as error:
        return log.error("reconcile failed reason=api_unreachable error=%r", str(error))
    accepted = known = 0
    for payment in reversed(payments["_embedded"]["payments"]):
        if payment["status"] != "paid":
            continue
        event = event_from(payment)
        if accept(event):
            accepted += 1
            log.warning(
                "reconcile recovered customer=%s payment=%s status=paid",
                event["customer"],
                payment["id"],
            )
        else:
            known += 1
    log.info(
        "reconcile finished recovered=%d already_known=%d%s",
        accepted,
        known,
        " worker=must_be_running_to_send_receipts" if accepted else "",
    )


async def main():
    if args.command == "webhooks":
        return await run_webhooks()
    if args.command == "report":
        return report(detail=True)
    if args.command == "reconcile":
        return reconcile()
    if args.reset:
        STATE.unlink(missing_ok=True)
        EVENTS.unlink(missing_ok=True)
        call("DELETE", "/v2/payments")
        return log.info("reset state=%s events=%s", STATE, EVENTS)
    if args.new_price:
        for customer in CUSTOMERS:
            if customer["amount"] != "0.00":
                customer["amount"] = "10.99"
    if args.plain:
        await run_plain()
    else:
        store = CheckpointStore(STATE)
        workflow = Workflow(bill, name="billing", key_function=lambda c: c["id"])
        stats = await workflow.run_many(CUSTOMERS, store, reporter=LogReporter(log))
        store.close()
        log.info(
            "run finished done=%d skipped=%d failed=%d",
            stats.items_done,
            stats.items_skipped,
            stats.items_failed,
        )
    report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        nargs="?",
        choices=["bill", "webhooks", "report", "reconcile"],
        default="bill",
    )
    parser.add_argument(
        "--random-keys", action="store_true", help="fresh Idempotency-Key per request"
    )
    parser.add_argument(
        "--plain", action="store_true", help="a bare for loop, without stitch"
    )
    parser.add_argument(
        "--new-price",
        action="store_true",
        help="bill 10.99 instead of 9.99: a rerun reuses the key with a different body",
    )
    parser.add_argument("--reset", action="store_true")
    parser.parse_args(namespace=args)
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    asyncio.run(main())
