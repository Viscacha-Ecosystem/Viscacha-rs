use std::sync::Arc;

use axum::extract::State;
use axum::response::Html;
use axum::Json;
use viscacha_core::JobStatus;
use viscacha_storage::PersistentSpace;

pub async fn dashboard_page() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

pub async fn dashboard_metrics(State(space): State<Arc<PersistentSpace>>) -> Json<serde_json::Value> {
    let all = space.list(None);

    let pending   = all.iter().filter(|j| j.status == JobStatus::Pending).count();
    let claimed   = all.iter().filter(|j| j.status == JobStatus::Claimed).count();
    let done      = all.iter().filter(|j| j.status == JobStatus::Done).count();
    let failed    = all.iter().filter(|j| j.status == JobStatus::Failed).count();
    let cancelled = all.iter().filter(|j| j.status == JobStatus::Cancelled).count();

    let with_start: Vec<_> = all.iter().filter(|j| j.started_at.is_some()).collect();
    let avg_wait = if with_start.is_empty() { 0.0 } else {
        with_start.iter().map(|j| j.started_at.unwrap() - j.enqueued_at).sum::<f64>()
            / with_start.len() as f64
    };

    let finished: Vec<_> = all.iter()
        .filter(|j| j.finished_at.is_some() && j.started_at.is_some())
        .collect();
    let avg_exec = if finished.is_empty() { 0.0 } else {
        finished.iter().map(|j| j.finished_at.unwrap() - j.started_at.unwrap()).sum::<f64>()
            / finished.len() as f64
    };

    Json(serde_json::json!({
        "total":             all.len(),
        "pending":           pending,
        "claimed":           claimed,
        "done":              done,
        "failed":            failed,
        "cancelled":         cancelled,
        "avg_queue_wait_ms": (avg_wait * 1000.0) as i64,
        "avg_exec_ms":       (avg_exec * 1000.0) as i64,
    }))
}

static DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
<title>Viscacha</title>
<style>
* { box-sizing: border-box; }
body { font-family: monospace; background: #0d0d0d; color: #ccc; margin: 0; padding: 8px; font-size: 13px; }
h2 { color: #fff; margin: 4px 0 8px 0; letter-spacing: 1px; }
#metrics { padding: 8px; background: #161616; border: 1px solid #2a2a2a; margin-bottom: 8px; white-space: pre; color: #aaa; }
#metrics span { margin-right: 20px; }
#metrics .v { color: #fff; }
.section { background: #161616; border: 1px solid #2a2a2a; margin-bottom: 8px; }
.section-header { padding: 4px 8px; background: #1e1e1e; color: #888; font-size: 11px; border-bottom: 1px solid #2a2a2a; }
#gantt-wrap { overflow-y: auto; max-height: 280px; padding: 4px; }
canvas { display: block; }
.controls { padding: 4px 8px; background: #1e1e1e; border-bottom: 1px solid #2a2a2a; display: flex; gap: 12px; align-items: center; }
input[type=text] { background: #111; border: 1px solid #333; color: #ccc; padding: 3px 6px; font-family: monospace; font-size: 12px; width: 220px; }
#count { color: #555; font-size: 11px; }
.table-wrap { overflow-y: auto; max-height: 340px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { background: #1e1e1e; color: #666; text-align: left; padding: 4px 8px; position: sticky; top: 0; font-weight: normal; border-bottom: 1px solid #2a2a2a; }
td { padding: 3px 8px; border-bottom: 1px solid #161616; cursor: pointer; }
tr:hover td { background: #1a1a1a; }
.pending   { color: #888; }
.claimed   { color: #cc0; }
.done      { color: #4c4; }
.failed    { color: #c44; }
.cancelled { color: #555; }
#err { color: #c44; padding: 4px 8px; display: none; }

/* ── detail / inspector panel ───────────────────────────────────────── */
#detail {
  position: fixed; right: 0; top: 0; height: 100vh; width: 480px;
  background: #111; border-left: 1px solid #2a2a2a;
  overflow-y: auto; display: none; font-size: 12px;
}
#d-topbar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 8px 10px; background: #161616; border-bottom: 1px solid #2a2a2a;
  position: sticky; top: 0; z-index: 1;
}
#d-job-type { font-size: 14px; color: #fff; font-weight: bold; }
#d-badge {
  display: inline-block; padding: 1px 7px; font-size: 11px;
  border-radius: 2px; margin-left: 8px; vertical-align: middle;
}
#d-badge.pending   { background: #0d1a2a; color: #6af; }
#d-badge.claimed   { background: #1e1e00; color: #cc0; }
#d-badge.done      { background: #061606; color: #4c4; }
#d-badge.failed    { background: #200808; color: #c44; }
#d-badge.cancelled { background: #141414; color: #666; }
#close-btn {
  background: none; border: none; color: #555; cursor: pointer;
  font-family: monospace; font-size: 13px; padding: 0; line-height: 1;
}
#close-btn:hover { color: #c44; }
#d-body { padding: 10px; }
#d-job-id { font-size: 10px; color: #333; margin-bottom: 10px; cursor: pointer; user-select: all; }
#d-job-id:hover { color: #666; }

/* timeline bar */
#d-bar-wrap { padding: 8px; background: #0a0a0a; border: 1px solid #1e1e1e; margin-bottom: 8px; }
#d-bar { position: relative; height: 10px; background: #1a1a1a; border-radius: 1px; }
#d-bar-queue { position: absolute; top: 0; bottom: 0; background: #2a2a2a; border-radius: 1px 0 0 1px; }
#d-bar-exec  { position: absolute; top: 0; bottom: 0; border-radius: 0 1px 1px 0; }
#d-stats { display: flex; flex-wrap: wrap; gap: 0 20px; margin-top: 6px; font-size: 11px; color: #444; }
#d-stats .sv { color: #aaa; }
#d-stats .sw { color: #c84; }

/* trace */
#d-trace { font-size: 11px; line-height: 2; padding: 6px 0; }
.te { display: block; }
.te-time { color: #333; }
.te-delta { color: #555; display: inline-block; width: 84px; }
.te-enqueued .te-ev { color: #888; }
.te-claimed  .te-ev { color: #cc0; }
.te-done     .te-ev { color: #4c4; }
.te-failed   .te-ev { color: #c44; }
.te-cancelled .te-ev { color: #666; }
.te-meta { color: #444; }

/* JSON sections */
.ds { border: 1px solid #1e1e1e; margin-bottom: 6px; }
.ds-hdr {
  display: flex; justify-content: space-between; align-items: center;
  padding: 3px 8px; background: #161616; color: #555; font-size: 10px;
  letter-spacing: 0.5px;
}
.ds-hdr button {
  background: none; border: none; color: #333; cursor: pointer;
  font-family: monospace; font-size: 10px; padding: 0;
}
.ds-hdr button:hover { color: #aaa; }
.ds pre {
  margin: 0; padding: 8px; font-size: 11px; line-height: 1.6;
  white-space: pre-wrap; word-break: break-all; color: #aaa;
  max-height: 260px; overflow-y: auto;
}
</style>
</head>
<body>
<h2>VISCACHA</h2>
<div id="err"></div>

<div id="metrics">connecting...</div>

<div class="section">
  <div class="section-header">GANTT  <span style="color:#444">(gray=queue wait  color=execution)</span></div>
  <div id="gantt-wrap"><canvas id="gantt" width="1800"></canvas></div>
</div>

<div class="section">
  <div class="controls">
    filter: <input type="text" id="filter" placeholder="type, status, or job id" oninput="render()">
    <span id="count"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>id</th><th>type</th><th>status</th>
        <th>queue wait</th><th>exec time</th><th>retries</th><th>worker</th><th>enqueued</th>
      </tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<!-- ── inspector ───────────────────────────────────────────────────── -->
<div id="detail">
  <div id="d-topbar">
    <div>
      <span id="d-job-type"></span>
      <span id="d-badge"></span>
    </div>
    <button id="close-btn" onclick="closeDetail()">× close</button>
  </div>

  <div id="d-body">
    <div id="d-job-id" title="click to copy" onclick="copyStr(this.textContent)"></div>

    <!-- timeline bar -->
    <div id="d-bar-wrap">
      <div id="d-bar">
        <div id="d-bar-queue"></div>
        <div id="d-bar-exec"></div>
      </div>
      <div id="d-stats"></div>
    </div>

    <!-- trace -->
    <div class="ds">
      <div class="ds-hdr">TRACE</div>
      <div id="d-trace"></div>
    </div>

    <!-- args -->
    <div class="ds">
      <div class="ds-hdr">ARGS <button onclick="copyEl('d-args')">copy</button></div>
      <pre id="d-args"></pre>
    </div>

    <!-- result -->
    <div class="ds" id="d-result-sec">
      <div class="ds-hdr">RESULT <button onclick="copyEl('d-result')">copy</button></div>
      <pre id="d-result"></pre>
    </div>

    <!-- error -->
    <div class="ds" id="d-error-sec">
      <div class="ds-hdr" style="color:#c44">ERROR</div>
      <pre id="d-error" style="color:#c44"></pre>
    </div>

    <!-- full json -->
    <div class="ds">
      <div class="ds-hdr">FULL JSON <button onclick="copyEl('d-full')">copy</button></div>
      <pre id="d-full"></pre>
    </div>
  </div>
</div>

<script>
let jobs = [];

// ── formatting helpers ────────────────────────────────────────────────

function fms(s) {
  if (s == null || s < 0) return '-';
  const ms = s * 1000;
  if (ms < 1000) return ms.toFixed(1) + 'ms';
  if (ms < 60000) return (ms / 1000).toFixed(2) + 's';
  return (ms / 60000).toFixed(1) + 'm';
}

function ftime(ts) {
  if (!ts) return '-';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString() + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function fdelta(t, t0) {
  const ms = (t - t0) * 1000;
  if (ms < 1000)  return '+' + ms.toFixed(1) + 'ms';
  if (ms < 60000) return '+' + (ms / 1000).toFixed(3) + 's';
  return '+' + (ms / 60000).toFixed(2) + 'm';
}

const STATUS_COLOR = {
  pending: '#555', claimed: '#cc0', done: '#4c4', failed: '#c44', cancelled: '#333'
};

// ── poll ──────────────────────────────────────────────────────────────

async function poll() {
  try {
    const [jr, mr] = await Promise.all([
      fetch('/jobs').then(r => r.json()),
      fetch('/dashboard/metrics').then(r => r.json()),
    ]);
    jobs = jr.jobs || [];

    const m = mr;
    document.getElementById('metrics').innerHTML =
      `<span>total <span class="v">${m.total}</span></span>` +
      `<span>pending <span class="v">${m.pending}</span></span>` +
      `<span>claimed <span class="v">${m.claimed}</span></span>` +
      `<span>done <span class="v">${m.done}</span></span>` +
      `<span>failed <span class="v">${m.failed}</span></span>` +
      `<span>cancelled <span class="v">${m.cancelled}</span></span>` +
      `  |  ` +
      `<span>avg wait <span class="v">${fms(m.avg_queue_wait_ms / 1000)}</span></span>` +
      `<span>avg exec <span class="v">${fms(m.avg_exec_ms / 1000)}</span></span>`;

    document.getElementById('err').style.display = 'none';
    render();

    // Refresh the open inspector if any job is being viewed
    const open = document.getElementById('detail').style.display !== 'none';
    if (open) {
      const id = document.getElementById('d-job-id').textContent;
      if (id) showDetail(id);
    }
  } catch (e) {
    const el = document.getElementById('err');
    el.textContent = 'error: ' + e.message;
    el.style.display = 'block';
  }
}

// ── render table + gantt ──────────────────────────────────────────────

function render() {
  const q = document.getElementById('filter').value.toLowerCase().trim();
  const filtered = q
    ? jobs.filter(j =>
        j.id.startsWith(q) ||
        j.job_type.toLowerCase().includes(q) ||
        j.status.includes(q)
      )
    : jobs;

  document.getElementById('count').textContent = `${filtered.length} / ${jobs.length}`;
  drawGantt(filtered);
  renderTable(filtered);
}

function drawGantt(visible) {
  const canvas = document.getElementById('gantt');
  const ctx    = canvas.getContext('2d');
  const ROW    = 8;
  const MAX    = 800;

  const shown = visible
    .slice()
    .sort((a, b) => (a.enqueued_at || 0) - (b.enqueued_at || 0))
    .slice(-MAX);

  canvas.height = Math.max(ROW * shown.length, 1);
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  if (shown.length === 0) return;

  const now   = Date.now() / 1000;
  const t_min = shown.reduce((m, j) => Math.min(m, j.enqueued_at || now), now);
  const t_max = shown.reduce((m, j) => Math.max(m, j.finished_at || now), t_min + 0.001);
  const range = t_max - t_min;
  const W     = canvas.width;
  const x = t => ((t - t_min) / range) * W;

  shown.forEach((job, i) => {
    const y = i * ROW;
    if (job.enqueued_at != null) {
      ctx.fillStyle = '#2a2a2a';
      ctx.fillRect(x(job.enqueued_at), y + 1,
        Math.max(1, x(job.started_at != null ? job.started_at : now) - x(job.enqueued_at)), ROW - 2);
    }
    if (job.started_at != null) {
      ctx.fillStyle = STATUS_COLOR[job.status] || '#888';
      ctx.fillRect(x(job.started_at), y + 1,
        Math.max(2, x(job.finished_at != null ? job.finished_at : now) - x(job.started_at)), ROW - 2);
    }
  });
}

function renderTable(visible) {
  const sorted = visible.slice().sort((a, b) => (b.enqueued_at || 0) - (a.enqueued_at || 0));
  document.getElementById('tbody').innerHTML = sorted.map(j => {
    const wait = (j.started_at != null && j.enqueued_at != null) ? j.started_at - j.enqueued_at : null;
    const exec = (j.finished_at != null && j.started_at != null) ? j.finished_at - j.started_at : null;
    const wid  = j.worker_id ? `<span style="color:#888">${j.worker_id}</span>` : `<span style="color:#333">—</span>`;
    return `<tr onclick="showDetail('${j.id}')">
      <td style="color:#333;font-size:10px">${j.id.slice(0, 8)}</td>
      <td>${j.job_type}</td>
      <td class="${j.status}">${j.status}</td>
      <td>${fms(wait)}</td>
      <td>${fms(exec)}</td>
      <td style="color:${j.retries > 0 ? '#c84' : '#333'}">${j.retries}</td>
      <td style="font-size:10px">${wid}</td>
      <td style="color:#444">${ftime(j.enqueued_at)}</td>
    </tr>`;
  }).join('');
}

// ── inspector ─────────────────────────────────────────────────────────

function showDetail(id) {
  const job = jobs.find(j => j.id === id);
  if (!job) return;

  // header
  document.getElementById('d-job-type').textContent = job.job_type;
  const badge = document.getElementById('d-badge');
  badge.textContent = job.status;
  badge.className   = job.status;

  // id (click-to-copy)
  document.getElementById('d-job-id').textContent = job.id;

  // timeline bar
  const now   = Date.now() / 1000;
  const t0    = job.enqueued_at || now;
  const t1    = job.finished_at || now;
  const range = Math.max(t1 - t0, 0.0001);

  const qFrac = job.started_at != null ? (job.started_at - t0) / range : 1;
  const eFrac = (job.finished_at != null && job.started_at != null)
    ? (job.finished_at - job.started_at) / range : 0;

  const bq = document.getElementById('d-bar-queue');
  const be = document.getElementById('d-bar-exec');
  bq.style.left  = '0';
  bq.style.width = (qFrac * 100).toFixed(2) + '%';
  be.style.left  = (qFrac * 100).toFixed(2) + '%';
  be.style.width = (eFrac * 100).toFixed(2) + '%';
  be.style.background = STATUS_COLOR[job.status] || '#888';

  // stats row
  const wait = job.started_at != null ? job.started_at - t0 : null;
  const exec = (job.finished_at != null && job.started_at != null) ? job.finished_at - job.started_at : null;
  let st = '';
  if (wait != null) st += `queue wait <span class="sv">${fms(wait)}</span>&nbsp;&nbsp;`;
  if (exec != null) st += `exec <span class="sv">${fms(exec)}</span>&nbsp;&nbsp;`;
  if (job.retries > 0) st += `retries <span class="sw">${job.retries}/${job.max_retries}</span>&nbsp;&nbsp;`;
  if (job.worker_id)  st += `worker <span class="sv">${job.worker_id}</span>`;
  document.getElementById('d-stats').innerHTML = st || '<span style="color:#333">waiting for worker…</span>';

  // trace
  document.getElementById('d-trace').innerHTML = buildTrace(job);

  // args
  document.getElementById('d-args').textContent = JSON.stringify(job.args, null, 2);

  // result
  const resSec = document.getElementById('d-result-sec');
  if (job.result != null) {
    document.getElementById('d-result').textContent = JSON.stringify(job.result, null, 2);
    resSec.style.display = '';
  } else {
    resSec.style.display = 'none';
  }

  // error
  const errSec = document.getElementById('d-error-sec');
  if (job.error != null) {
    document.getElementById('d-error').textContent = job.error;
    errSec.style.display = '';
  } else {
    errSec.style.display = 'none';
  }

  // full json
  document.getElementById('d-full').textContent = JSON.stringify(job, null, 2);

  // url hash so you can link/refresh to this job
  history.replaceState(null, '', '#job=' + id);
  document.getElementById('detail').style.display = 'block';
}

function buildTrace(job) {
  const t0  = job.enqueued_at;
  const now = Date.now() / 1000;
  const row = (cls, time, delta, ev, meta) =>
    `<span class="te te-${cls}">` +
    `<span class="te-time">${ftime(time)}</span>  ` +
    `<span class="te-delta">${delta.padEnd(12)}</span>` +
    `<span class="te-ev">${ev}</span>` +
    (meta ? `  <span class="te-meta">${meta}</span>` : '') +
    `</span>`;

  const lines = [row('enqueued', t0, 'T+0', 'ENQUEUED', '')];

  if (job.started_at != null) {
    const w = job.worker_id ? `worker: ${job.worker_id}` : '';
    lines.push(row('claimed', job.started_at, fdelta(job.started_at, t0), 'CLAIMED', w));

    if (job.finished_at != null) {
      const exec = `exec: ${fms(job.finished_at - job.started_at)}`;
      const ev   = job.status === 'done' ? 'DONE'
                 : job.status === 'failed' ? 'FAILED'
                 : 'CANCELLED';
      lines.push(row(job.status, job.finished_at, fdelta(job.finished_at, t0), ev, exec));
    } else {
      lines.push(row('claimed', now, fdelta(now, t0), '… running', `elapsed: ${fms(now - job.started_at)}`));
    }
  } else if (job.status === 'cancelled' && job.finished_at != null) {
    lines.push(row('cancelled', job.finished_at, fdelta(job.finished_at, t0), 'CANCELLED', ''));
  }

  return lines.join('');
}

function closeDetail() {
  history.replaceState(null, '', window.location.pathname);
  document.getElementById('detail').style.display = 'none';
}

function copyEl(id) {
  navigator.clipboard.writeText(document.getElementById(id).textContent).catch(() => {});
}

function copyStr(s) {
  navigator.clipboard.writeText(s).catch(() => {});
}

// restore inspector from URL hash on load
function checkHash() {
  const m = window.location.hash.match(/^#job=(.+)$/);
  if (m && jobs.length) showDetail(m[1]);
}

poll();
setInterval(poll, 1000);
setTimeout(checkHash, 200);
</script>
</body>
</html>"#;
