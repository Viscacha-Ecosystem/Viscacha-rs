# viscacha-rs

A high-performance job queue server written in Rust. Drop-in backend for the
[viscacha Python SDK](https://pypi.org/project/viscacha/), or use it standalone
via its HTTP API.

Zero broker, Redis, or sidecar. It is just one binary, and one SQLite file.

---

## Why

The Python library runs jobs in-process, which is great for prototyping and
single-machine pipelines. When you need to scale workers across machines, add
persistence that survives restarts, or hand jobs off to non-Python services,
you will need a server.

`viscacha-rs` is that server! It speaks the same HTTP protocol the Python SDK
already knows, so switching is one line of code:

```python
# Before: in-process
client = Client()

# After: backed by the Rust server
client = Client(url="http://localhost:8000")
```

Everything else: `enqueue`, `wait`, `cancel`, the `Worker` decorator, and the
dashboard all stays exactly the same.

---

## Features

- **Crash-safe by default.** Every operation is appended to a SQLite WAL
  event log before it touches in-memory state. On restart, the log replays
  deterministically, and the queue is fully restored.

- **Lease-based claiming.** Workers hold a timed lease on each job. If a
  worker crashes or stalls, the lease expires and the job is automatically
  returned to the queue with no manual intervention required.

- **Automatic retries.** Jobs that fail are re-queued up to `max_retries`
  times, then marked permanently failed. The full retry history is preserved.

- **Periodic snapshots.** The event log is compacted automatically. Startup
  time stays constant regardless of how many jobs have run.

- **Clean HTTP API.** Standard JSON over HTTP. Any language or HTTP client.
  OpenAPI coming.

- **Zero dependencies to deploy.** A single statically-linked binary. No
  runtime, VM, or container required (although it runs fine in one).

---

## Quick Start

### Build

```bash
git clone https://github.com/SkylarM-B/Viscacha
cd viscacha-rs
cargo build --release -p viscacha-api
```

### Run

```bash
# Persistent — state survives restarts
./target/release/viscacha jobs.db

# In memory — useful for testing
./target/release/viscacha

# Custom bind address
./target/release/viscacha jobs.db 0.0.0.0:9000
```

The server prints the address it is listening on and is ready immediately!

### Use with Python

```bash
pip install viscacha requests
```

```python
from viscacha import Client, Worker

client = Client(url="http://localhost:8000")
worker = Worker(client)

@worker.job("process_document", max_retries=3, lease_ttl=60.0)
def process_document(path: str) -> dict:
    # ... your stuff ...
    return {"pages": 12}

worker.run(blocking=False)

handle = client.enqueue("process_document", path="report.pdf")
result = handle.wait(timeout=120)
print(result.result)  # {"pages": 12}
```

Workers can run on any machine that can reach the server. Enqueue from one
process, consume from another, check status from a third; all using the same
Python SDK!

---

## HTTP API

All endpoints accept and return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Enqueue a job |
| `GET` | `/jobs` | List jobs (optional `?status=` filter) |
| `GET` | `/jobs/{id}` | Get a single job by ID |
| `POST` | `/jobs/{id}/cancel` | Cancel a pending job |
| `POST` | `/jobs/claim` | Claim the next available job (used by Worker) |
| `POST` | `/jobs/{id}/complete` | Mark a claimed job done |
| `POST` | `/jobs/{id}/fail` | Mark a claimed job failed |

### Enqueue

```http
POST /jobs
Content-Type: application/json

{
  "job_type": "send_email",
  "args": { "to": "billybob@example.com" },
  "max_retries": 3
}
```

```json
{ "job_id": "b3d2f1a0-..." }
```

### Job object

All read endpoints return jobs in this shape:

```json
{
  "id":          "b3d2f1a0-...",
  "status":      "done",
  "job_type":    "send_email",
  "args":        { "to": "joeschmo@example.com" },
  "result":      { "message_id": "msg_123" },
  "error":       null,
  "retries":     0,
  "max_retries": 3,
  "enqueued_at": 1745000000.0,
  "started_at":  1745000001.2,
  "finished_at": 1745000002.8
}
```

**Status values:** `pending` | `claimed` | `done` | `failed` | `cancelled`

---

## Architecture

```
                    HTTP (JSON)
Client / Worker  <------------->  viscacha-rs
                                      |
                              +-------+-------+
                              |  TupleSpace   |  <- in-memory projection
                              |  (DashMap)    |
                              +-------+-------+
                                      |
                              +-------+-------+
                              |  Event Log    |  <- append-only source of truth
                              |  (SQLite WAL) |
                              +-------+-------+
                                      |
                              +-------+-------+
                              |   Snapshots   |  <- periodic compaction
                              +---------------+
```

**Event sourcing** Every mutation (enqueue, claim, complete, fail, cancel,
expire) is written to the event log before it is applied in memory. The
in-memory state is always a pure projection of the log. Crash at any point yet
replay rebuilds the exact same state.

**Lease reaper** A background tokio task scans for expired leases on a fixed
interval. Expired jobs are returned to `pending` and an `Expire` event is
appended so the expiry survives the next restart.

**Snapshots** Periodically, the current job state is serialized to the
`snapshots` table and events older than that snapshot are truncated. This
bounds replay time and disk usage.

---

## Workspace Layout

```
crates/
  core/      Job types, state machine, in memory TupleSpace, lease reaper
  storage/   SQLite event log, snapshots, replay, PersistentSpace wrapper
  api/       Axum HTTP server, route handlers, request/response models
```

The crates are independently testable. `core` has no I/O. `storage` depends
only on `core`. `api` depends on both. This makes it straightforward to swap
storage backends or add alternate transports later.

---

## Development

```bash
# Run all tests
cargo test --workspace

# Run tests for one crate
cargo test -p viscacha-core
cargo test -p viscacha-storage
cargo test -p viscacha-api

# Start a dev server (in-memory, no file)
cargo run -p viscacha-api
```

The test suite covers the full lifecycle end-to-end:

| Crate | Tests | Coverage |
|-------|-------|----------|
| `viscacha-core` | 14 | State machine, lease expiry, all error paths |
| `viscacha-storage` | 11 | Event log, snapshots, crash recovery, replay |
| `viscacha-api` | 11 | All HTTP endpoints, error responses, full cycle |

---

## Roadmap

- [ ] Lease reaper wired into `PersistentSpace` (currently targets `TupleSpace` directly)
- [ ] `cargo run` dev server with `--watch` flag for local development
- [ ] OpenAPI generation
- [ ] Multi-tenancy via API key middleware
- [ ] Prometheus metrics endpoint
- [ ] Configurable snapshot interval and log retention

---

## Relation to the Python SDK

`viscacha-rs` implements the same wire protocol that `viscacha` uses when its
initialized with a `url=` argument. The Python SDK is the reference
implementation and the source of the protocol contract. The Rust server
is a production grade backend for it.

If you are building a pure-Python pipeline that runs on a single machine,
the Python SDK alone is sufficient enough. You should reach for `viscacha-rs` when you need:

- Workers on separate machines
- Persistence across process restarts
- Higher throughput than a Python server can provide (limited by GIL)
- A not Python service enqueueing or consuming jobs

---

## License

MIT
