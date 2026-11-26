# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `idempotency_key(scope="")` returns a stable key for the current run, item and
  step. It stays the same on retries, reruns and heals, so a payment provider can
  recognise a request it already handled (sent as an `Idempotency-Key` header).
- `Workflow.accept(item, store)` records an item as `pending` in one short
  transaction and returns `False` for a key it has already seen. Use it to answer
  webhooks fast and do the work later.
- `Workflow.process_pending(store)` runs everything accepted but not finished,
  including parked items that are due.
- `examples/payments`: a payment client (`billing.py`) and a local Mollie-style API (`fake_payments_api.py`)
  to try killing the script, restarting the server and rerunning, with and without `idempotency_key()`.

### Changed

- `WorkflowRecovery.resume()` also picks up `pending` items.

## [0.2.0] - 2026-10-20

### Added

- `CheckpointStore.delete_run(run_id)` removes one run's items, checkpoints and audit records.
- `CheckpointStore.delete()` closes the store and removes its SQLite files.
- Docstrings on the public API for IDE hover help.
- README use cases showing stitch added to existing pipelines.

### Changed

- `@step` and `Workflow` raise `TypeError` when given a plain `def` instead of
  an `async def`, rather than failing later with "object can't be awaited".

## [0.1.0]

Initial release.

### Added

- `@step` checkpoints with per-item resume after a crash or rerun.
- Retry policies with exponential backoff, jitter and server `retry_after` hints.
- Fallbacks, degradation and parking for items whose dependencies are down.
- Critical steps with health checks that halt the run.
- Provider pools with circuit breakers and failover.
- SQLite audit trail, run reporting and trace rendering.
- Workflow recovery for parked and failed items.
- Pluggable `Alerter` for run alerts.

[Unreleased]: https://github.com/batman004/stitch/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/batman004/stitch/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/batman004/stitch/releases/tag/v0.1.0
