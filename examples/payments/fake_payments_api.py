"""A tiny local stand-in for a Mollie-style payments API. Not Mollie. Run: python fake_payments_api.py

POST /v2/payments   create a payment
GET  /v2/payments/tr_... one payment (needs `Authorization: Bearer test_...`)
GET  /v2/payments   list payments, newest first
DELETE /v2/payments forget everything (demo reset)

Behaviour copied from how Mollie documents it:
* `Idempotency-Key`: the same key and body replays the first response with `Idempotent-Replayed: true`;
  the same key with a different body is 400.
* State is saved to payments.json, so stopping and restarting the server keeps the payments.
* After creating a payment the server waits DELAY seconds before replying: kill the client in that
  window and you have a payment the client never recorded.
* If the payment has a `webhookUrl`, it is marked `paid` WEBHOOK_AFTER seconds later and the URL gets
  `id=tr_...` (form encoded, like Mollie's classic webhooks) twice, as retried deliveries do.
  A call that fails is tried again a few times, so you can restart your receiver.
* Invalid input is 422. Every 5th create call is answered 429 with `Retry-After: 1` to show backoff.
"""

import json
import logging
import os
import secrets
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8765
RATE_LIMIT_EVERY = 5
DELAY = 2.0
WEBHOOK_AFTER = 3.0
STATE = Path("payments.json")

saved = (
    json.loads(STATE.read_text()) if STATE.exists() else {"payments": [], "keys": {}}
)
payments: list[dict] = saved["payments"]
keys: dict[str, list] = saved[
    "keys"
]  # idempotency key -> [request body as json, payment]
calls = {"create": 0}

log = logging.getLogger("payments_api")


def send_webhook(payment_id, url):
    time.sleep(WEBHOOK_AFTER)
    for payment in payments:
        if payment["id"] == payment_id:
            payment["status"] = "paid"
    save()
    data = urllib.parse.urlencode({"id": payment_id}).encode()
    for delivery in range(2):
        for attempt in range(5):
            try:
                urllib.request.urlopen(
                    urllib.request.Request(url, data=data, method="POST"), timeout=5
                )
                log.info(
                    "webhook delivered payment=%s delivery=%d/2",
                    payment_id,
                    delivery + 1,
                )
                break
            except OSError as error:
                log.warning(
                    "webhook delivery failed payment=%s attempt=%d error=%r retry_in=2s",
                    payment_id,
                    attempt + 1,
                    error,
                )
                time.sleep(2)
        else:
            log.error("webhook given up payment=%s url=%s attempts=5", payment_id, url)


def save():
    STATE.write_text(json.dumps({"payments": payments, "keys": keys}))


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/hal+json")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)

    def error(self, status, title, detail, headers=None):
        self.reply(
            status, {"status": status, "title": title, "detail": detail}, headers
        )

    def authorised(self):
        if self.headers.get("Authorization", "").startswith("Bearer test_"):
            return True
        self.error(401, "Unauthorized Request", "Use a test_ API key.")
        return False

    def do_POST(self):
        if self.path != "/v2/payments" or not self.authorised():
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        key, fingerprint = self.headers.get("Idempotency-Key"), json.dumps(
            body, sort_keys=True
        )

        if key in keys:
            first_body, first_payment = keys[key]
            if first_body != fingerprint:
                log.warning(
                    "payment refused status=400 reason=idempotency_key_mismatch "
                    "idempotency_key=%s original_payment=%s",
                    key,
                    first_payment["id"],
                )
                return self.error(
                    400,
                    "Bad Request",
                    "Idempotency key reused with a different request.",
                )
            log.info(
                "payment replayed payment=%s idempotency_key=%s",
                first_payment["id"],
                key,
            )
            return self.reply(201, first_payment, {"Idempotent-Replayed": "true"})

        calls["create"] += 1
        if calls["create"] % RATE_LIMIT_EVERY == 0:
            log.warning("rate limited status=429 retry_after=1")
            return self.error(
                429, "Too Many Requests", "Slow down.", {"Retry-After": "1"}
            )
        try:
            if float(body["amount"]["value"]) <= 0:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            log.info(
                "payment refused status=422 reason=invalid_amount description=%r",
                body.get("description"),
            )
            return self.error(422, "Unprocessable Entity", "The amount is invalid.")

        payment = {
            "resource": "payment",
            "id": "tr_" + secrets.token_urlsafe(6),
            "status": "open",
            **body,
        }
        payments.append(payment)
        if key:
            keys[key] = [fingerprint, payment]
        save()
        log.info(
            "payment created payment=%s description=%r reply_in=%ss",
            payment["id"],
            body.get("description"),
            DELAY,
        )
        if body.get("webhookUrl"):
            threading.Thread(
                target=send_webhook,
                args=(payment["id"], body["webhookUrl"]),
                daemon=True,
            ).start()
        time.sleep(DELAY)
        self.reply(201, payment)

    def do_GET(self):
        if self.path.startswith("/v2/payments/") and self.authorised():
            found = [p for p in payments if p["id"] == self.path.rsplit("/", 1)[1]]
            return (
                self.reply(200, found[0])
                if found
                else self.error(404, "Not Found", "No such payment.")
            )
        if self.path.startswith("/v2/payments") and self.authorised():
            self.reply(
                200, {"count": len(payments), "_embedded": {"payments": payments[::-1]}}
            )

    def do_DELETE(self):
        if self.path == "/v2/payments" and self.authorised():
            payments.clear(), keys.clear()
            calls["create"] = 0
            save()
            log.info("state reset")
            self.reply(200, {"reset": True})

    def log_message(self, format, *args):
        log.debug("http %s %s", self.address_string(), format % args)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    log.info("listening url=http://127.0.0.1:%d state=%s", PORT, STATE)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
