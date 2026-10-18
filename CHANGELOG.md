# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `CheckpointStore.delete_run(run_id)` removes one run's items, checkpoints and audit records.
- `CheckpointStore.delete()` closes the store and removes its SQLite files.
- Docstrings on the public API for IDE hover help.
- README use cases showing stitch added to existing pipelines.

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

[Unreleased]: https://github.com/batman004/stitch/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/batman004/stitch/releases/tag/v0.1.0
