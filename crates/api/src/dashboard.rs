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
input[type=text] { background: #111; border: 1px solid #333; color: #ccc; padding: 3px 6px; font-family: monospace; font-size: 12px; width: 180px; }
#count { color: #555; font-size: 11px; }
.table-wrap { overflow-y: auto; max-height: 380px; }
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th { background: #1e1e1e; color: #666; text-align: left; padding: 4px 8px; position: sticky; top: 0; font-weight: normal; border-bottom: 1px solid #2a2a2a; }
td { padding: 3px 8px; border-bottom: 1px solid #161616; cursor: pointer; }
tr:hover td { background: #1a1a1a; }
.pending  { color: #888; }
.claimed  { color: #cc0; }
.done     { color: #4c4; }
.failed   { color: #c44; }
.cancelled{ color: #555; }
#detail { position: fixed; right: 0; top: 0; height: 100vh; width: 420px; background: #111; border-left: 1px solid #2a2a2a; padding: 12px; overflow-y: auto; display: none; font-size: 12px; }
#detail h3 { margin: 0 0 8px 0; color: #fff; font-size: 13px; }
#detail pre { white-space: pre-wrap; word-break: break-all; color: #aaa; font-size: 11px; line-height: 1.5; }
#close-btn { float: right; cursor: pointer; color: #c44; background: none; border: none; font-family: monospace; font-size: 13px; }
#err { color: #c44; padding: 4px 8px; display: none; }
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
    filter: <input type="text" id="filter" placeholder="type or status" oninput="render()">
    <span id="count"></span>
  </div>
  <div class="table-wrap">
    <table>
      <thead><tr>
        <th>id</th><th>type</th><th>status</th>
        <th>queue wait</th><th>exec time</th><th>retries</th><th>enqueued</th>
      </tr></thead>
      <tbody id="tbody"></tbody>
    </table>
  </div>
</div>

<div id="detail">
  <button id="close-btn" onclick="document.getElementById('detail').style.display='none'">x close</button>
  <h3 id="d-title"></h3>
  <pre id="d-body"></pre>
</div>

<script>
let jobs = [];

function fms(s) {
  if (s == null || s < 0) return '-';
  const ms = s * 1000;
  if (ms < 1000) return ms.toFixed(0) + 'ms';
  if (ms < 60000) return (ms / 1000).toFixed(2) + 's';
  return (ms / 60000).toFixed(1) + 'm';
}

function ftime(ts) {
  if (!ts) return '-';
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString() + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

const STATUS_COLOR = {
  pending: '#555', claimed: '#cc0', done: '#4c4', failed: '#c44', cancelled: '#333'
};

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
  } catch (e) {
    const el = document.getElementById('err');
    el.textContent = 'error: ' + e.message;
    el.style.display = 'block';
  }
}

function render() {
  const q = document.getElementById('filter').value.toLowerCase().trim();
  const filtered = q
    ? jobs.filter(j => j.job_type.toLowerCase().includes(q) || j.status.includes(q))
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
      const x0 = x(job.enqueued_at);
      const x1 = x(job.started_at != null ? job.started_at : now);
      ctx.fillStyle = '#2a2a2a';
      ctx.fillRect(x0, y + 1, Math.max(1, x1 - x0), ROW - 2);
    }

    if (job.started_at != null) {
      const x0 = x(job.started_at);
      const x1 = x(job.finished_at != null ? job.finished_at : now);
      ctx.fillStyle = STATUS_COLOR[job.status] || '#888';
      ctx.fillRect(x0, y + 1, Math.max(2, x1 - x0), ROW - 2);
    }
  });
}

function renderTable(visible) {
  const sorted = visible
    .slice()
    .sort((a, b) => (b.enqueued_at || 0) - (a.enqueued_at || 0));

  document.getElementById('tbody').innerHTML = sorted.map(j => {
    const wait = (j.started_at != null && j.enqueued_at != null)
      ? j.started_at - j.enqueued_at : null;
    const exec = (j.finished_at != null && j.started_at != null)
      ? j.finished_at - j.started_at : null;
    return `<tr onclick="showDetail('${j.id}')">
      <td style="color:#444;font-size:11px">${j.id.slice(0, 8)}</td>
      <td>${j.job_type}</td>
      <td class="${j.status}">${j.status}</td>
      <td>${fms(wait)}</td>
      <td>${fms(exec)}</td>
      <td style="color:${j.retries > 0 ? '#c84' : '#555'}">${j.retries}</td>
      <td style="color:#555">${ftime(j.enqueued_at)}</td>
    </tr>`;
  }).join('');
}

function showDetail(id) {
  const job = jobs.find(j => j.id === id);
  if (!job) return;
  document.getElementById('d-title').textContent = `${job.job_type}  [${job.status}]`;
  document.getElementById('d-body').textContent  = JSON.stringify(job, null, 2);
  document.getElementById('detail').style.display = 'block';
}

poll();
setInterval(poll, 1000);
</script>
</body>
</html>"#;
