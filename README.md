# viscacha-rs

Reliable event-sourced background job server written in Rust. Drop-in backend for the
[viscacha Python SDK](https://pypi.org/project/viscacha/), or use it standalone
via its HTTP API.

No broker. Redis. or sidecar. One binary + one SQLite file.

---

## Why

The Python library runs jobs in-process, which is great for development and
single machine pipelines. When you need to scale workers across machines, add
persistence that survives restarts, or hand jobs off to non-Python services,
you need a server.

`viscacha-rs` is that server! It speaks the same HTTP protocol the Python SDK
already knows, so switching is one line of code:

```python
# Before: in-process
client = Client()

# After: backed by the Rust server
client = Client(url="http://localhost:8000")
```

Everything else — `enqueue`, `wait`, `cancel`, the `Worker` decorator —
stays exactly the same.

---

## Features

- **Crash safe by default.** Every operation is appended to a SQLite WAL
  event log before it touches in-memory state. On restart, the log replays
  deterministically and the queue is fully restored.

- **Lease based claiming.** Workers hold a timed lease on each job. If a
  worker crashes or stalls, the lease expires and the job is automatically
  returned to the queue, no manual intervention required.

- **Automatic retries.** Jobs that fail are re-queued up to `max_retries`
  times, then marked permanently failed. The full retry history is preserved.

- **Periodic snapshots.** The event log is compacted automatically. Startup
  time stays constant regardless of how many jobs have run.

- **Clean HTTP API.** Standard JSON over HTTP. Any language, any HTTP client.

- **Zero dependencies to deploy.** A single statically-linked binary. No
  runtime, no VM, no container required (although it runs fine in one).

- **Built-in observability.** Live HTML dashboard, Prometheus metrics endpoint,
  per-job trace timeline, and worker attribution all included, no plugins
  or sidecars needed.

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

# In-memory — useful for testing
./target/release/viscacha

# Custom bind address
./target/release/viscacha jobs.db 0.0.0.0:9000
```

The server prints the address it is listening on and is ready immediately.

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
    # ... do work ...
    return {"pages": 12}

worker.run(blocking=False)

handle = client.enqueue("process_document", path="report.pdf")
result = handle.wait(timeout=120)
print(result.result)  # {"pages": 12}
```

Workers can run on any machine that can reach the server. Enqueue from one
process, consume from another, and check status from a third all using the same
Python SDK.

---

## Observability

### Built-in dashboard — `/dashboard`

Open `http://localhost:8000/dashboard` in any browser. No setup required.

- Live job list that refreshes every two seconds, with filter by ID prefix, type, or status
- Click any row to open the job inspector:
  - Proportional timeline bar showing time in queue vs. execution time
  - Structured TRACE view with timestamped events (enqueued → claimed → done/failed)
  - ARGS, RESULT, and ERROR sections with one click copy buttons
  - Full raw JSON with copy button
- URL hash support — `http://localhost:8000/dashboard#job=<id>` links directly to any job

### Prometheus metrics — `/metrics`

`http://localhost:8000/metrics` serves standard Prometheus text format. Scrape
it with any compatible collector.

| Metric | Type | Description |
|--------|------|-------------|
| `viscacha_jobs{status}` | Gauge | Job count by status |
| `viscacha_retried_jobs` | Gauge | Jobs retried at least once |
| `viscacha_queue_wait_seconds` | Histogram | Time from enqueue to claim |
| `viscacha_exec_seconds` | Histogram | Time from claim to completion or failure |
| `viscacha_worker_jobs{worker_id, status}` | Gauge | Jobs attributed per worker |
| `viscacha_job_start_seconds{job_id, job_type, worker_id}` | Gauge | Per job claim timestamp |
| `viscacha_job_end_seconds{job_id, job_type, worker_id, status}` | Gauge | Per job finish timestamp |

### Grafana dashboard

A fully provisioned Grafana + Prometheus stack is included via Docker Compose:

```bash
docker compose up -d
```

Grafana opens at `http://localhost:9001` (no login required in dev mode).
The dashboard is provisioned automatically with 12 panels:

- **Stat tiles** — live counts for pending, active, done, failed, cancelled, retried
- **Job counts by status** — time series showing queue depth over time
- **Queue wait time** — p50 / p95 / p99 latency from enqueue to claim
- **Execution time** — p50 / p95 / p99 latency from claim to completion
- **Execution time heatmap** — distribution of execution times over time
- **Jobs by worker** — time series broken out per `worker_id`
- **Job trace table** — all jobs in memory with worker, type, status, and timestamps;
  clicking a row opens the job inspector in the built-in dashboard

### Worker attribution

Pass `worker_id` when claiming a job to enable per worker metrics and
traceability:

```http
POST /jobs/claim
Content-Type: application/json

{
  "job_type":  "render_row",
  "lease_ttl": 30.0,
  "worker_id": "gpu-node-3-12345"
}
```

The `worker_id` is stored on the job, surfaced in `/metrics`, and displayed
in both the built-in dashboard and Grafana. For Python workers:

```bash
python run_workers.py --n 10        # spawns 10 worker processes, each with a unique ID
python mandelbrot.py --worker       # single worker process
```

### Job trace — `/jobs/{id}/trace`

Returns a structured timeline for any job:

```http
GET /jobs/b3d2f1a0-.../trace
```

```json
{
  "job": { ... },
  "timeline": [
    { "event": "enqueued", "at": 1745000000.0, "offset_ms": 0 },
    { "event": "claimed",  "at": 1745000001.2, "offset_ms": 1200, "worker_id": "gpu-node-3-12345" },
    { "event": "done",     "at": 1745000002.8, "offset_ms": 2800, "exec_ms": 1600, "result": { ... } }
  ]
}
```

---

## HTTP API

All endpoints accept and return JSON.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Enqueue a job |
| `GET` | `/jobs` | List jobs (optional `?status=` filter) |
| `GET` | `/jobs/{id}` | Get a single job by ID |
| `POST` | `/jobs/{id}/cancel` | Cancel a pending job |
| `POST` | `/jobs/claim` | Claim the next available job |
| `POST` | `/jobs/{id}/complete` | Mark a claimed job done |
| `POST` | `/jobs/{id}/fail` | Mark a claimed job failed |
| `GET` | `/jobs/{id}/trace` | Get the full event timeline for a job |
| `GET` | `/dashboard` | Built-in live HTML dashboard |
| `GET` | `/metrics` | Prometheus metrics endpoint |

### Enqueue

```http
POST /jobs
Content-Type: application/json

{
  "job_type": "send_email",
  "args": { "to": "alice@example.com" },
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
  "args":        { "to": "alice@example.com" },
  "result":      { "message_id": "msg_123" },
  "error":       null,
  "retries":     0,
  "max_retries": 3,
  "worker_id":   "gpu-node-3-12345",
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
                              |  (RwLock)     |
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

**Event sourcing.** Every mutation (enqueue, claim, complete, fail, cancel,
expire) is written to the event log before it is applied in memory. The
in-memory state is always a pure projection of the log. Crash at any point;
replay rebuilds the exact same state.

**Lease reaper.** A background tokio task scans for expired leases on a fixed
interval. Expired jobs are returned to `pending` and an `Expire` event is
appended so the expiry survives the next restart.

**Snapshots.** Periodically the current job state is serialized to the
`snapshots` table and events older than that snapshot are truncated. This
bounds replay time and disk usage.

---

## Workspace Layout

```
crates/
  core/      Job types, state machine, in-memory TupleSpace, lease reaper
  storage/   SQLite event log, snapshots, replay, PersistentSpace wrapper
  api/       Axum HTTP server, route handlers, request/response models,
             built-in dashboard, Prometheus metrics
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
- [ ] OpenAPI spec generation
- [ ] Multi-tenancy via API key middleware
- [ ] Configurable snapshot interval and log retention

---

## Relation to the Python SDK

`viscacha-rs` implements the same wire protocol that `viscacha` uses when
initialized with a `url=` argument. The Python SDK is the reference
implementation and the source of the protocol contract. The Rust server
is a production-grade backend for it.

If you are building a pure-Python pipeline that runs on a single machine,
the Python SDK alone is sufficient. Reach for `viscacha-rs` when you need:

- Workers on separate machines
- Persistence across process restarts
- Higher throughput than a Python server can provide (limited by GIL)
- A non-Python service enqueueing or consuming jobs

---

## License

MIT
