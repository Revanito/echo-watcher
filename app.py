import csv
import io
import math
import os
import re
import sqlite3
import statistics
import subprocess
import threading
import time
from collections import deque

from flask import Flask, jsonify, request, Response

# ---------------------------------------------------------------------------
# Config (all overridable via environment variables)
# ---------------------------------------------------------------------------
TARGETS = [t.strip() for t in os.environ.get("TARGETS", "1.1.1.1,8.8.8.8").split(",") if t.strip()]
INTERVAL = float(os.environ.get("INTERVAL", "1"))
PING_TIMEOUT = float(os.environ.get("PING_TIMEOUT", "1"))
OUTAGE_FAILS = int(os.environ.get("OUTAGE_FAILS", "2"))
SPIKE_ABS_MS = float(os.environ.get("SPIKE_ABS_MS", "150"))
SPIKE_JITTER_MS = float(os.environ.get("SPIKE_JITTER_MS", "40"))
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "14"))
DB_PATH = os.environ.get("DB_PATH", "/data/ping.db")
PORT = int(os.environ.get("PORT", "8531"))

RTT_RE = re.compile(r"time[=<]([\d.]+)\s*ms")

db_lock = threading.Lock()
_conn = None


def get_conn():
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _conn.execute("PRAGMA journal_mode=WAL")
    return _conn


def init_db():
    conn = get_conn()
    with db_lock:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS pings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                target TEXT NOT NULL,
                rtt_ms REAL
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pings_target_ts ON pings(target, ts)")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                target TEXT NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER,
                detail TEXT
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_target_ts ON events(target, start_ts)")
        conn.commit()


def insert_ping(ts, target, rtt_ms):
    conn = get_conn()
    with db_lock:
        conn.execute("INSERT INTO pings (ts, target, rtt_ms) VALUES (?, ?, ?)", (ts, target, rtt_ms))
        conn.commit()


def insert_event(type_, target, start_ts, end_ts, detail):
    conn = get_conn()
    with db_lock:
        cur = conn.execute(
            "INSERT INTO events (type, target, start_ts, end_ts, detail) VALUES (?, ?, ?, ?, ?)",
            (type_, target, start_ts, end_ts, detail),
        )
        conn.commit()
        return cur.lastrowid


def close_event(event_id, end_ts, detail):
    conn = get_conn()
    with db_lock:
        conn.execute("UPDATE events SET end_ts = ?, detail = ? WHERE id = ?", (end_ts, detail, event_id))
        conn.commit()


def ping_once(target):
    """Runs one ICMP echo via the system ping binary. Returns rtt in ms, or None on timeout/loss."""
    timeout_s = max(1, math.ceil(PING_TIMEOUT))
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", str(timeout_s), target],
            capture_output=True,
            text=True,
            timeout=timeout_s + 1,
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    match = RTT_RE.search(result.stdout)
    if not match:
        return None
    return float(match.group(1))


def ping_loop(target):
    consecutive_fails = 0
    open_outage_id = None
    baseline = deque(maxlen=20)

    while True:
        loop_start = time.monotonic()
        ts = int(time.time())
        rtt = ping_once(target)
        insert_ping(ts, target, rtt)

        if rtt is None:
            consecutive_fails += 1
            if consecutive_fails == OUTAGE_FAILS and open_outage_id is None:
                open_outage_id = insert_event(
                    "outage", target, ts, None, f"{OUTAGE_FAILS} consecutive timeouts"
                )
        else:
            if open_outage_id is not None:
                # find the start_ts already stored to compute duration
                conn = get_conn()
                with db_lock:
                    row = conn.execute("SELECT start_ts FROM events WHERE id = ?", (open_outage_id,)).fetchone()
                start_ts = row[0] if row else ts
                duration = ts - start_ts
                close_event(open_outage_id, ts, f"down for {duration}s ({OUTAGE_FAILS}+ consecutive timeouts)")
                open_outage_id = None
            consecutive_fails = 0

            if len(baseline) >= 5:
                median = statistics.median(baseline)
                if rtt >= SPIKE_ABS_MS or (rtt - median) >= SPIKE_JITTER_MS:
                    insert_event(
                        "spike", target, ts, ts,
                        f"rtt={rtt:.1f}ms baseline={median:.1f}ms",
                    )
            baseline.append(rtt)

        elapsed = time.monotonic() - loop_start
        time.sleep(max(0.0, INTERVAL - elapsed))


def cleanup_loop():
    while True:
        cutoff = int(time.time()) - RETENTION_DAYS * 86400
        conn = get_conn()
        with db_lock:
            conn.execute("DELETE FROM pings WHERE ts < ?", (cutoff,))
            conn.commit()
        time.sleep(6 * 3600)


# ---------------------------------------------------------------------------
# Web app
# ---------------------------------------------------------------------------
app = Flask(__name__)

RANGE_SECONDS = {"1h": 3600, "24h": 86400, "7d": 7 * 86400, "30d": 30 * 86400}


def summary_for(target, since_ts):
    conn = get_conn()
    with db_lock:
        total, fails, avg_rtt, max_rtt = conn.execute(
            """SELECT COUNT(*), SUM(CASE WHEN rtt_ms IS NULL THEN 1 ELSE 0 END),
                      AVG(rtt_ms), MAX(rtt_ms)
               FROM pings WHERE target = ? AND ts >= ?""",
            (target, since_ts),
        ).fetchone()
        outage_count, outage_downtime = conn.execute(
            """SELECT COUNT(*), COALESCE(SUM(COALESCE(end_ts, strftime('%s','now')) - start_ts), 0)
               FROM events WHERE target = ? AND type = 'outage' AND start_ts >= ?""",
            (target, since_ts),
        ).fetchone()
        spike_count = conn.execute(
            "SELECT COUNT(*) FROM events WHERE target = ? AND type = 'spike' AND start_ts >= ?",
            (target, since_ts),
        ).fetchone()[0]

    total = total or 0
    fails = fails or 0
    loss_pct = (fails / total * 100) if total else 0.0
    return {
        "target": target,
        "samples": total,
        "loss_pct": round(loss_pct, 2),
        "avg_rtt_ms": round(avg_rtt, 1) if avg_rtt is not None else None,
        "max_rtt_ms": round(max_rtt, 1) if max_rtt is not None else None,
        "uptime_pct": round(100 - loss_pct, 2),
        "outage_count": outage_count or 0,
        "outage_downtime_s": outage_downtime or 0,
        "spike_count": spike_count or 0,
    }


@app.route("/api/summary")
def api_summary():
    range_key = request.args.get("range", "24h")
    since_ts = int(time.time()) - RANGE_SECONDS.get(range_key, 86400)
    return jsonify([summary_for(t, since_ts) for t in TARGETS])


@app.route("/api/series")
def api_series():
    target = request.args.get("target", TARGETS[0])
    range_key = request.args.get("range", "1h")
    seconds = RANGE_SECONDS.get(range_key, 3600)
    since_ts = int(time.time()) - seconds
    max_points = 600
    bucket = max(1, seconds // max_points)

    conn = get_conn()
    with db_lock:
        rows = conn.execute(
            """SELECT (ts / ?) * ? AS bucket,
                      AVG(rtt_ms), COUNT(*), SUM(CASE WHEN rtt_ms IS NULL THEN 1 ELSE 0 END)
               FROM pings WHERE target = ? AND ts >= ?
               GROUP BY bucket ORDER BY bucket""",
            (bucket, bucket, target, since_ts),
        ).fetchall()

    points = [
        {
            "ts": r[0],
            "avg_rtt_ms": round(r[1], 1) if r[1] is not None else None,
            "count": r[2],
            "loss": r[3],
        }
        for r in rows
    ]
    return jsonify({"target": target, "bucket_seconds": bucket, "points": points})


@app.route("/api/events")
def api_events():
    target = request.args.get("target")
    range_key = request.args.get("range", "7d")
    since_ts = int(time.time()) - RANGE_SECONDS.get(range_key, 7 * 86400)
    limit = int(request.args.get("limit", "200"))

    conn = get_conn()
    query = "SELECT id, type, target, start_ts, end_ts, detail FROM events WHERE start_ts >= ?"
    params = [since_ts]
    if target:
        query += " AND target = ?"
        params.append(target)
    query += " ORDER BY start_ts DESC LIMIT ?"
    params.append(limit)

    with db_lock:
        rows = conn.execute(query, params).fetchall()

    return jsonify(
        [
            {"id": r[0], "type": r[1], "target": r[2], "start_ts": r[3], "end_ts": r[4], "detail": r[5]}
            for r in rows
        ]
    )


@app.route("/export/events.csv")
def export_events_csv():
    conn = get_conn()
    with db_lock:
        rows = conn.execute(
            "SELECT type, target, start_ts, end_ts, detail FROM events ORDER BY start_ts"
        ).fetchall()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["type", "target", "start_time_utc", "end_time_utc", "detail"])
    for type_, target, start_ts, end_ts, detail in rows:
        writer.writerow(
            [
                type_,
                target,
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(start_ts)),
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(end_ts)) if end_ts else "",
                detail,
            ]
        )
    return Response(buf.getvalue(), mimetype="text/csv",
                     headers={"Content-Disposition": "attachment; filename=ping-monitor-events.csv"})


@app.route("/")
def dashboard():
    return DASHBOARD_HTML.replace("__TARGETS__", ",".join(TARGETS))


DASHBOARD_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Ping Monitor</title>
<style>
  body { font-family: system-ui, sans-serif; background: #0e1117; color: #e6e6e6; margin: 0; padding: 24px; }
  h1 { font-size: 20px; margin-bottom: 4px; }
  .sub { color: #888; font-size: 13px; margin-bottom: 20px; }
  .controls { margin-bottom: 16px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  select, button, a.btn {
    background: #1b1f27; color: #e6e6e6; border: 1px solid #333; border-radius: 6px;
    padding: 6px 12px; font-size: 13px; cursor: pointer; text-decoration: none;
  }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 20px; }
  .card { background: #161a22; border: 1px solid #262b36; border-radius: 8px; padding: 12px 16px; min-width: 130px; }
  .card .label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: .04em; }
  .card .value { font-size: 22px; font-weight: 600; margin-top: 4px; }
  .warn { color: #f0a020; } .bad { color: #e05252; } .ok { color: #4caf7a; }
  canvas { background: #10131a; border: 1px solid #262b36; border-radius: 8px; width: 100%; height: 220px; display: block; }
  .chart-wrap { position: relative; }
  .tooltip {
    position: absolute; pointer-events: none; transform: translate(-50%, -100%);
    background: #1b1f27; border: 1px solid #333; border-radius: 6px; padding: 6px 10px;
    font-size: 12px; white-space: nowrap; margin-top: -8px;
  }
  .tooltip .t { color: #888; font-size: 11px; }
  table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #262b36; }
  th { color: #888; font-weight: 500; }
  .tag { border-radius: 4px; padding: 2px 6px; font-size: 11px; }
  .tag.outage { background: #4a1f22; color: #f28b8b; }
  .tag.spike { background: #4a3a1f; color: #f2c98b; }
</style>
</head>
<body>
  <h1>Ping Monitor</h1>
  <div class="sub">Monitoring: __TARGETS__ &middot; refreshes every 10s &middot; <span id="updated">loading…</span></div>

  <div class="controls">
    <select id="target"></select>
    <select id="range">
      <option value="1h">last 1h</option>
      <option value="24h" selected>last 24h</option>
      <option value="7d">last 7d</option>
      <option value="30d">last 30d</option>
    </select>
    <a class="btn" href="/export/events.csv">Download events CSV</a>
  </div>

  <div class="cards" id="cards"></div>
  <div class="chart-wrap">
    <canvas id="chart" height="220"></canvas>
    <div class="tooltip" id="tooltip" hidden></div>
  </div>

  <h3>Recent outages &amp; spikes</h3>
  <table>
    <thead><tr><th>Type</th><th>Target</th><th>Start (UTC)</th><th>End (UTC)</th><th>Detail</th></tr></thead>
    <tbody id="events"></tbody>
  </table>

<script>
const targets = "__TARGETS__".split(",");
const targetSel = document.getElementById("target");
targets.forEach(t => {
  const o = document.createElement("option"); o.value = t; o.textContent = t; targetSel.appendChild(o);
});
const rangeSel = document.getElementById("range");
const tooltip = document.getElementById("tooltip");

let currentPoints = [];
let currentRange = "1h";
let currentGeom = null;

function fmtTime(ts) {
  if (!ts) return "";
  return new Date(ts * 1000).toISOString().replace("T", " ").slice(0, 19);
}

async function refresh() {
  const target = targetSel.value;
  const range = rangeSel.value;
  const noStore = { cache: "no-store" };

  const summaries = await fetch(`/api/summary?range=${range}`, noStore).then(r => r.json());
  const s = summaries.find(x => x.target === target) || summaries[0];
  const cards = document.getElementById("cards");
  const uptimeClass = s.uptime_pct >= 99.9 ? "ok" : s.uptime_pct >= 99 ? "warn" : "bad";
  cards.innerHTML = `
    <div class="card"><div class="label">Uptime</div><div class="value ${uptimeClass}">${s.uptime_pct}%</div></div>
    <div class="card"><div class="label">Packet loss</div><div class="value">${s.loss_pct}%</div></div>
    <div class="card"><div class="label">Avg latency</div><div class="value">${s.avg_rtt_ms ?? "-"} ms</div></div>
    <div class="card"><div class="label">Max latency</div><div class="value">${s.max_rtt_ms ?? "-"} ms</div></div>
    <div class="card"><div class="label">Outages</div><div class="value ${s.outage_count ? 'bad' : 'ok'}">${s.outage_count} (${s.outage_downtime_s}s)</div></div>
    <div class="card"><div class="label">Spikes</div><div class="value ${s.spike_count ? 'warn' : 'ok'}">${s.spike_count}</div></div>
  `;

  const series = await fetch(`/api/series?target=${target}&range=${range}`, noStore).then(r => r.json());
  currentPoints = series.points;
  currentRange = range;
  currentGeom = drawChart(currentPoints, currentRange);

  const events = await fetch(`/api/events?target=${target}&range=${range}&limit=100`, noStore).then(r => r.json());
  const tbody = document.getElementById("events");
  tbody.innerHTML = events.map(e => `
    <tr>
      <td><span class="tag ${e.type}">${e.type}</span></td>
      <td>${e.target}</td>
      <td>${fmtTime(e.start_ts)}</td>
      <td>${fmtTime(e.end_ts)}</td>
      <td>${e.detail || ""}</td>
    </tr>`).join("") || "<tr><td colspan=5>No events in this range.</td></tr>";

  document.getElementById("updated").textContent =
    "last updated " + new Date().toLocaleTimeString();
}

function fmtAxisTime(ts, range) {
  const d = new Date(ts * 1000);
  const time = d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
  if (range === "7d" || range === "30d") {
    return d.toLocaleDateString(undefined, { month: "numeric", day: "numeric" }) + " " + time;
  }
  return time;
}

function drawChart(points, range) {
  const canvas = document.getElementById("chart");
  const ctx = canvas.getContext("2d");
  const w = canvas.width = canvas.clientWidth;
  const h = canvas.height = 220;
  ctx.clearRect(0, 0, w, h);
  if (!points.length) return null;

  const values = points.map(p => p.avg_rtt_ms).filter(v => v !== null);
  const maxRtt = Math.max(10, ...values, 1);
  const n = points.length;
  const padL = 40, padB = 20, padT = 10;
  const plotW = w - padL - 10, plotH = h - padB - padT;

  ctx.strokeStyle = "#333"; ctx.fillStyle = "#888"; ctx.font = "10px sans-serif";
  for (let i = 0; i <= 4; i++) {
    const y = padT + plotH - (i / 4) * plotH;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - 10, y); ctx.stroke();
    ctx.fillText(Math.round((i / 4) * maxRtt) + "ms", 2, y + 3);
  }

  // x-axis time labels
  const tickCount = Math.min(5, n);
  ctx.fillStyle = "#888";
  for (let k = 0; k < tickCount; k++) {
    const i = tickCount === 1 ? 0 : Math.round((k / (tickCount - 1)) * (n - 1));
    const x = padL + (i / n) * plotW;
    ctx.textAlign = k === 0 ? "left" : k === tickCount - 1 ? "right" : "center";
    ctx.fillText(fmtAxisTime(points[i].ts, range), x, h - 4);
  }
  ctx.textAlign = "left";

  // loss bars
  ctx.fillStyle = "rgba(224,82,82,0.5)";
  points.forEach((p, i) => {
    if (p.count > 0 && p.loss > 0) {
      const x = padL + (i / n) * plotW;
      ctx.fillRect(x, padT, Math.max(1, plotW / n), plotH);
    }
  });

  // latency line
  ctx.strokeStyle = "#4caf7a"; ctx.lineWidth = 1.5; ctx.beginPath();
  let started = false;
  points.forEach((p, i) => {
    if (p.avg_rtt_ms === null) { started = false; return; }
    const x = padL + (i / n) * plotW;
    const y = padT + plotH - (p.avg_rtt_ms / maxRtt) * plotH;
    if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); }
  });
  ctx.stroke();

  return { padL, padT, plotW, plotH, maxRtt, n };
}

function fmtFullTime(ts) {
  return new Date(ts * 1000).toLocaleString(undefined, {
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function onChartHover(e) {
  if (!currentGeom || !currentPoints.length) return;
  const canvas = document.getElementById("chart");
  const rect = canvas.getBoundingClientRect();
  const mouseX = e.clientX - rect.left;
  const { padL, padT, plotW, plotH, maxRtt, n } = currentGeom;

  let i = Math.floor(((mouseX - padL) / plotW) * n);
  i = Math.max(0, Math.min(n - 1, i));
  const p = currentPoints[i];
  const x = padL + (i / n) * plotW;

  drawChart(currentPoints, currentRange);
  const ctx = canvas.getContext("2d");
  ctx.strokeStyle = "rgba(255,255,255,0.35)";
  ctx.setLineDash([3, 3]);
  ctx.beginPath(); ctx.moveTo(x, padT); ctx.lineTo(x, padT + plotH); ctx.stroke();
  ctx.setLineDash([]);
  if (p.avg_rtt_ms !== null) {
    const y = padT + plotH - (p.avg_rtt_ms / maxRtt) * plotH;
    ctx.fillStyle = "#4caf7a";
    ctx.beginPath(); ctx.arc(x, y, 3.5, 0, Math.PI * 2); ctx.fill();
  }

  const rttText = p.avg_rtt_ms !== null ? `${p.avg_rtt_ms} ms` : "no reply";
  const lossText = p.loss > 0 ? ` &middot; ${p.loss}/${p.count} lost` : "";
  tooltip.innerHTML = `<div class="t">${fmtFullTime(p.ts)}</div><div>${rttText}${lossText}</div>`;
  tooltip.hidden = false;

  const pointY = p.avg_rtt_ms !== null ? (padT + plotH - (p.avg_rtt_ms / maxRtt) * plotH) : padT;
  tooltip.style.top = pointY + "px";
  tooltip.style.left = x + "px";

  const wrapRect = canvas.parentElement.getBoundingClientRect();
  const tw = tooltip.offsetWidth;
  if (x - tw / 2 < 0) tooltip.style.left = (tw / 2) + "px";
  if (x + tw / 2 > wrapRect.width) tooltip.style.left = (wrapRect.width - tw / 2) + "px";
}

function onChartLeave() {
  tooltip.hidden = true;
  if (currentGeom) drawChart(currentPoints, currentRange);
}

document.getElementById("chart").addEventListener("mousemove", onChartHover);
document.getElementById("chart").addEventListener("mouseleave", onChartLeave);

targetSel.addEventListener("change", refresh);
rangeSel.addEventListener("change", refresh);
refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>
"""


def main():
    init_db()
    for target in TARGETS:
        threading.Thread(target=ping_loop, args=(target,), daemon=True).start()
    threading.Thread(target=cleanup_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)


if __name__ == "__main__":
    main()
