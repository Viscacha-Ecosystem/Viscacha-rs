# viscacha-rs

A job queue server written in Rust. Works as a drop-in backend for the
[viscacha Python SDK](https://pypi.org/project/viscacha/), or standalone via HTTP.

No broker, Redis, or sidecar. One binary + SQLite file.

| Tool | Complexity | Infra Burden |
|---|---|---|
| Celery | Medium-high | Redis / RabbitMQ |
| RQ | Medium | Redis |
| Temporal | Very high | Heavy |
| **Viscacha** | Low | None |

---

## Why

The Python library runs jobs in-process, which is fine for a single machine. When
you need workers on separate machines, persistence across restarts, or jobs coming
from non-Python services, you need a server.

`viscacha-rs` is that server. It speaks the same HTTP protocol the Python SDK
already knows, so switching is one line:

```python
# Before
client = Client()

# After
client = Client(url="http://localhost:8000")
```

Everything else stays the same: `enqueue`, `wait`, `cancel`, the `Worker` decorator, all of it.

---

## Features

- **Crash-safe.** Every operation is written to a SQLite WAL event log before touching
  in-memory state. On restart, the log replays and the queue is fully restored.

- **Lease-based claiming.** Workers hold a timed lease on each job. If a worker crashes
  or goes silent, the lease expires and the job goes back to the queue automatically.

- **Automatic retries.** Failed jobs are re-queued up to `max_retries` times, then
  marked permanently failed. The full retry history is preserved.

- **Snapshots.** The event log is compacted periodically, so startup time stays
  constant no matter how many jobs have run.

- **Clean HTTP API.** Standard JSON over HTTP. Any language, any HTTP client.

- **Single binary.** Statically linked, no runtime dependencies. Runs fine in a
  container but doesn't require one.

- **Built-in observability.** Time-travel debugger, live ops dashboard, Prometheus
  metrics, per-job trace timelines, and worker attribution. No plugins needed.

---

## Quick Start

### Docker (recommended)

```bash
git clone https://github.com//Viscacha-Ecosystem/Viscacha
cd viscacha-rs
docker compose up
```

- Queue + time-travel UI: `http://localhost:8000`
- Grafana dashboards: `http://localhost:9001`

### Build from source

```bash
git clone https://github.com//Viscacha-Ecosystem/Viscacha
cd viscacha-rs
cargo build --release -p viscacha-api
```

```bash
# Persistent (state survives restarts)
./target/release/viscacha jobs.db

# In-memory (useful for testing)
./target/release/viscacha

# Custom bind address
./target/release/viscacha jobs.db 0.0.0.0:9000
```

The server prints its bind address when it's ready.

### Use with Python

```bash
pip install viscacha requests
```

```python
from viscacha import Client, Worker

client = Client(url="http://localhost:8000")
# With auth enabled on the server:
# client = Client(url="http://localhost:8000", api_key="your-key")
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
process, consume from another, check status from a third.

---

## Observability

### Time-travel debugger (`/`)

Open `http://localhost:8000` in any browser. Three tabs:

- **LIVE** - auto-refreshing job table. Click any job to open the inspector.
  Failed jobs get a retry button. "View events" jumps to that job's full audit trail.
- **TIME MACHINE** - pick any past timestamp and see the exact queue state at that moment.
  The event strip shows the last hour of activity as colored bars; click anywhere to jump there.
- **EVENT LOG** - load events by time range or job ID. Click any row to jump to
  Time Machine at that timestamp.

You can also link directly to any job: `http://localhost:8000/#job=<id>`

### Live ops dashboard (`/dashboard`)

`http://localhost:8000/dashboard` has a Gantt chart, filterable job table, and a
per-job inspector with a timeline bar and trace.

### Prometheus metrics (`/metrics`)

`http://localhost:8000/metrics` serves standard Prometheus text format.

| Metric | Type | Description |
|--------|------|-------------|
| `viscacha_jobs{status}` | Gauge | Job count by status |
| `viscacha_retried_jobs` | Gauge | Jobs retried at least once |
| `viscacha_queue_wait_seconds` | Histogram | Time from enqueue to claim |
| `viscacha_exec_seconds` | Histogram | Time from claim to completion or failure |
| `viscacha_worker_jobs{worker_id, status}` | Gauge | Jobs attributed per worker |
| `viscacha_job_start_seconds{job_id, job_type, worker_id}` | Gauge | Per-job claim timestamp |
| `viscacha_job_end_seconds{job_id, job_type, worker_id, status}` | Gauge | Per-job finish timestamp |

### Grafana dashboard

`docker compose up -d` spins up a fully provisioned Grafana + Prometheus stack.
Grafana is at `http://localhost:9001` (no login in dev mode) with 12 panels:

- Stat tiles: live counts for pending, active, done, failed, cancelled, retried
- Job counts by status over time
- Queue wait time: p50 / p95 / p99
- Execution time: p50 / p95 / p99
- Execution time heatmap
- Jobs by worker
- Job trace table (click a row to open the inspector)

### Worker attribution

Pass `worker_id` when claiming to get per-worker metrics:

```http
POST /jobs/claim
Content-Type: application/json

{
  "job_type":  "render_row",
  "lease_ttl": 30.0,
  "worker_id": "gpu-node-3-12345"
}
```

The `worker_id` is stored on the job and shows up in `/metrics`, the dashboard, and Grafana.
Python workers get a unique ID automatically; you can also set one explicitly:

```python
worker = Worker(client, worker_id="gpu-node-3")
```

### Job trace (`/jobs/{id}/trace`)

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

### OpenAPI spec (`/openapi.json`)

`http://localhost:8000/openapi.json` serves the full OpenAPI 3.x spec. Import it
into Swagger UI, Postman, Insomnia, or any other tool.

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
| `POST` | `/jobs/{id}/retry` | Re-queue a permanently-failed job |
| `POST` | `/jobs/{id}/heartbeat` | Extend a claimed job's lease |
| `GET` | `/jobs/{id}/trace` | Structured timeline for a job |
| `GET` | `/jobs/{id}/events` | Full event log for a job |
| `GET` | `/replay` | Reconstruct queue state at `?at=<unix_ts>` |
| `GET` | `/replay/events` | Events in `?from=<ts>&to=<ts>&limit=<n>` |
| `GET` | `/` | Time-travel debugger UI |
| `GET` | `/dashboard` | Live ops dashboard |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/openapi.json` | OpenAPI spec |

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

Status values: `pending`, `claimed`, `done`, `failed`, `cancelled`

### Authentication

Set `VISCACHA_API_KEY` on the server to require a bearer token on all requests.
Leave it unset for open access (fine for local dev, not for production).

```python
client = Client(url="http://...", api_key="your-key")
```

```http
Authorization: Bearer your-key
```

The UI (`/`) and spec (`/openapi.json`) are always public.

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

Every mutation (enqueue, claim, complete, fail, cancel, expire) is written to the
event log before it's applied in memory. The in-memory state is always a pure
projection of the log. Crash at any point and replay rebuilds the exact same state.

A background task scans for expired leases on a fixed interval and returns stale
jobs to `pending`. An `Expire` event is appended so the expiry survives a restart.

Periodically the current job state is snapshotted and old events are truncated,
which keeps replay time and disk usage bounded.

---

## Workspace layout

```
crates/
  core/      Job types, state machine, in-memory TupleSpace, lease reaper
  storage/   SQLite event log, snapshots, replay, PersistentSpace wrapper
  api/       Axum HTTP server, route handlers, request/response models,
             built-in dashboard, Prometheus metrics
```

`core` has no I/O. `storage` depends only on `core`. `api` depends on both.

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

| Crate | Tests | What's covered |
|-------|-------|----------------|
| `viscacha-core` | 14 | State machine, lease expiry, all error paths |
| `viscacha-storage` | 11 | Event log, snapshots, crash recovery, replay |
| `viscacha-api` | 11 | All HTTP endpoints, error responses, full cycle |

---

## Relation to the Python SDK

`viscacha-rs` implements the wire protocol that `viscacha` uses when you pass `url=`.
The Python SDK is the reference implementation. The Rust server is the production backend.

If you're running a single-machine Python pipeline, the SDK alone is enough. Reach
for `viscacha-rs` when you need workers on separate machines, persistence, higher
throughput, or jobs coming from a non-Python service.

---

## License

MIT
