Chessgraph API + ingest layout
==============================

This module hosts the Go services and shared code:

- `cmd/ingest/` – streaming PGN importer that writes graph updates
- `cmd/api/` – HTTP API serving graph queries and a visualization UI
- `internal/` – shared packages: storage, graph model, ingest pipeline, eval, HTTP handlers, logging helpers

The UI lives separately under `/ui/` (not scaffolded here yet).

High-level responsibilities
- BadgerDB-based graph storage keyed by node/move
- Incremental ingest: PGN stream → edge deltas → Badger batch writes
- Optional Stockfish eval workers writing `V:<nodeID>` records
- HTTP API with request ID + structured logging

