"""
enhancements_ext.py
-------------------
Additive layer over enhancements.py. Wired from main.py via register_ext().

Adds:
  * Rolling per-server stats history (in-memory ring, ~24h @ 5min)
  * /server/stats-history/<folder>            GET
  * /server/log-range/<folder>?minutes=N      GET (raw text)
  * /server/schedule-backup/<folder>          GET/POST toggle auto-backup
  * /user/notify-config                       GET/POST webhook config
  * /servers/overview                         GET  (JSON multi-bot summary)
  * /overview                                 GET  (HTML page)
  * Crash/restart webhook ping (Discord/webhook + optional email hook)
  * Background scheduler thread (auto-backup + stats sampler)

Non-destructive: nothing here removes or replaces existing routes.
"""
from __future__ import annotations
import os, io, time, json, sqlite3, threading, datetime, zipfile, shutil, urllib.request, urllib.error
from collections import deque
from typing import Dict, Any

from flask import request, jsonify, session, abort, send_file, render_template_string, Response

import enhancements as _enh  # re-use its DATA_DIR + private helpers where safe

DATA_DIR = _enh.DATA_DIR

# ── In-memory state ───────────────────────────────────────────────
# folder -> deque[(ts, cpu, ram_mb, sys_cpu, sys_ram, online)]
_stats_hist: Dict[str, deque] = {}
_HIST_MAX = 288  # 24h @ 5-min
_last_sample: Dict[str, float] = {}

# folder -> {"enabled": bool, "interval_h": int, "last_run": float}
_backup_sched: Dict[str, Dict[str, Any]] = {}

_ext_lock = threading.Lock()


def _now_ts() -> float:
    return time.time()


def _ensure_ext_schema():
    os.makedirs(os.path.join(DATA_DIR, "storage"), exist_ok=True)
    db_path = os.path.join(DATA_DIR, "storage", "nehost.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE IF NOT EXISTS notify_config (
        user_id INTEGER PRIMARY KEY,
        webhook_url TEXT,
        email TEXT,
        events TEXT DEFAULT 'crash,restart',
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS backup_schedule (
        folder TEXT PRIMARY KEY,
        enabled INTEGER DEFAULT 0,
        interval_h INTEGER DEFAULT 24,
        last_run TIMESTAMP
    )""")
    conn.commit()
    conn.close()


def _db():
    return sqlite3.connect(os.path.join(DATA_DIR, "storage", "nehost.db"))


# ── Hook into enhancements' _push_stats to capture history ────────
def _install_stats_tap():
    orig = _enh._push_stats

    def _tapped(socketio, folder, running_procs, start_times,
                orphan_pids, process_is_ours, get_db):
        try:
            orig(socketio, folder, running_procs, start_times,
                 orphan_pids, process_is_ours, get_db)
        finally:
            _sample_stats(folder, running_procs)

    _enh._push_stats = _tapped


def _sample_stats(folder: str, running_procs):
    """Sample once every ~5 min (300s) per folder into the ring."""
    now = _now_ts()
    last = _last_sample.get(folder, 0)
    if now - last < 300:
        # still push tiny "live" sparkline data every 30s
        if now - last < 30:
            return
    _last_sample[folder] = now
    try:
        import psutil
        vm = psutil.virtual_memory()
        cpu_sys = psutil.cpu_percent(interval=None)
        online = folder in running_procs and running_procs[folder].poll() is None
        cpu_p = 0.0; ram_p = 0.0
        if online:
            try:
                p = psutil.Process(running_procs[folder].pid)
                cpu_p = p.cpu_percent(interval=0.0)
                ram_p = round(p.memory_info().rss / (1024 * 1024), 1)
            except Exception:
                pass
        with _ext_lock:
            q = _stats_hist.setdefault(folder, deque(maxlen=_HIST_MAX))
            q.append({
                "t": int(now), "cpu": cpu_p, "ram_mb": ram_p,
                "sys_cpu": cpu_sys, "sys_ram_pct": vm.percent,
                "online": bool(online),
            })
    except Exception:
        pass


# ── Notifications (webhook/Discord) ───────────────────────────────
def _fire_webhook(user_id: int, event: str, payload: Dict[str, Any]):
    try:
        conn = _db(); conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT webhook_url, events FROM notify_config WHERE user_id=?",
                           (user_id,)).fetchone()
        conn.close()
        if not row or not row["webhook_url"]:
            return
        events = (row["events"] or "").split(",")
        if event not in events:
            return
        url = row["webhook_url"]
        body = {
            "content": f"**[{event.upper()}]** {payload.get('folder','?')} — {payload.get('msg','')}",
            "embeds": [{
                "title": f"{event}: {payload.get('folder','')}",
                "description": payload.get("msg", ""),
                "color": 0xff4d5e if event == "crash" else 0x22e07a,
                "footer": {"text": f"CLAUD panel · {datetime.datetime.utcnow().isoformat()}Z"},
            }],
        }
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=6).read()
        except urllib.error.HTTPError:
            pass
    except Exception:
        pass


def _install_restart_notifier(socketio, get_db):
    """Wrap enhancements._do_auto_restart to also fire notifications."""
    orig = _enh._do_auto_restart

    def _wrapped(folder, running_procs, start_times):
        orig(folder, running_procs, start_times)
        try:
            conn = _db(); conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT user_id FROM servers WHERE folder=?",
                               (folder,)).fetchone()
            conn.close()
            if row:
                _fire_webhook(int(row["user_id"]), "restart", {
                    "folder": folder, "msg": "Server auto-restarted after crash.",
                })
        except Exception:
            pass

    _enh._do_auto_restart = _wrapped


# ── Backup scheduler ──────────────────────────────────────────────
def _load_backup_schedules():
    try:
        conn = _db(); conn.row_factory = sqlite3.Row
        for r in conn.execute("SELECT * FROM backup_schedule").fetchall():
            _backup_sched[r["folder"]] = {
                "enabled": bool(r["enabled"]),
                "interval_h": int(r["interval_h"] or 24),
                "last_run": 0.0,
            }
        conn.close()
    except Exception:
        pass


def _run_backup(folder: str, base_storage: str):
    src = os.path.join(base_storage, folder)
    if not os.path.isdir(src):
        return
    backups_dir = os.path.join(DATA_DIR, "storage", "backups", folder)
    os.makedirs(backups_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(backups_dir, f"auto_{stamp}.zip")
    try:
        with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            for root, dirs, files in os.walk(src):
                if os.path.commonpath([root, backups_dir]) == backups_dir:
                    continue
                for fn in files:
                    if fn == "console.log":
                        continue
                    full = os.path.join(root, fn)
                    z.write(full, os.path.relpath(full, src))
        # Prune: keep last 8 auto_ backups
        autos = sorted([f for f in os.listdir(backups_dir) if f.startswith("auto_")])
        for old in autos[:-8]:
            try: os.remove(os.path.join(backups_dir, old))
            except Exception: pass
    except Exception:
        pass


def _start_scheduler(base_storage: str):
    def loop():
        while True:
            try:
                now = _now_ts()
                for folder, cfg in list(_backup_sched.items()):
                    if not cfg.get("enabled"):
                        continue
                    interval = max(1, int(cfg.get("interval_h", 24))) * 3600
                    if now - cfg.get("last_run", 0) >= interval:
                        _run_backup(folder, base_storage)
                        cfg["last_run"] = now
                        try:
                            conn = _db()
                            conn.execute("UPDATE backup_schedule SET last_run=CURRENT_TIMESTAMP WHERE folder=?",
                                         (folder,))
                            conn.commit(); conn.close()
                        except Exception:
                            pass
            except Exception:
                pass
            time.sleep(60)
    t = threading.Thread(target=loop, name="backup-sched", daemon=True)
    t.start()


# ── Multi-bot overview ────────────────────────────────────────────
OVERVIEW_HTML = """
<!doctype html><html><head><meta charset="utf-8"/>
<title>Servers overview</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css"/>
<style>
body{background:#0a0f1a;color:#e6edf7;font-family:'Inter',system-ui,sans-serif;margin:0;padding:16px}
h1{font-size:16px;margin:6px 0 14px;display:flex;align-items:center;gap:8px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}
.card{background:#101827;border:1px solid #253560;border-radius:12px;padding:12px;position:relative}
.dot{width:9px;height:9px;border-radius:50%;background:#6b7280;display:inline-block;margin-right:6px}
.dot.on{background:#22e07a;box-shadow:0 0 8px rgba(34,224,122,.6)}
.dot.off{background:#ff4d5e}
.n{font-weight:700;font-size:13px}
.m{font-size:11px;color:#7f8aa3;margin-top:4px}
.up{font-family:'JetBrains Mono',monospace;font-size:11px;color:#7ec8ff;margin-top:6px}
a{color:inherit;text-decoration:none;display:block}
.back{color:#7ec8ff;font-size:12px}
</style></head><body>
<a class="back" href="/dashboard">&larr; Back to dashboard</a>
<h1><i class="fas fa-layer-group"></i> Your servers</h1>
<div class="grid" id="g">Loading…</div>
<script>
fetch('/servers/overview').then(r=>r.json()).then(({servers})=>{
  const g=document.getElementById('g'); g.innerHTML='';
  if(!servers||!servers.length){g.innerHTML='<div class="m">No servers yet.</div>';return;}
  servers.forEach(s=>{
    const el=document.createElement('a');
    el.href='/dashboard?folder='+encodeURIComponent(s.folder);
    el.innerHTML=`<div class="card">
      <div><span class="dot ${s.online?'on':'off'}"></span><span class="n">${s.name}</span></div>
      <div class="m">${s.folder}</div>
      <div class="up">Uptime last 24h: ${s.uptime_pct}%</div>
      <div class="m">Samples: ${s.samples}</div>
    </div>`;
    g.appendChild(el);
  });
});
</script></body></html>
"""


# ── Public register ───────────────────────────────────────────────
def register_ext(app, socketio, context):
    """Called from main.py right after enhancements.register()."""
    _ensure_ext_schema()
    _load_backup_schedules()
    _install_stats_tap()
    _install_restart_notifier(socketio, context["get_db"])
    _start_scheduler(context["BASE_STORAGE"])

    auth_check              = context["auth_check"]
    verify_folder_ownership = context["verify_folder_ownership"]
    check_csrf              = context["check_csrf"]
    get_db                  = context["get_db"]
    BASE_STORAGE            = context["BASE_STORAGE"]

    # ── stats history ────────────────────────────────────────────
    @app.route("/server/stats-history/<folder>")
    def stats_history(folder):
        if not auth_check():
            return jsonify({"samples": []}), 403
        verify_folder_ownership(folder)
        with _ext_lock:
            q = list(_stats_hist.get(folder, ()))
        return jsonify({"folder": folder, "samples": [s["cpu"] for s in q],
                        "full": q})

    # ── raw log slice by minutes ─────────────────────────────────
    @app.route("/server/log-range/<folder>")
    def log_range(folder):
        if not auth_check():
            abort(403)
        verify_folder_ownership(folder)
        try:
            minutes = max(1, min(360, int(request.args.get("minutes", "5"))))
        except ValueError:
            minutes = 5
        log_p = os.path.join(BASE_STORAGE, folder, "console.log")
        if not os.path.exists(log_p):
            return Response("", mimetype="text/plain")
        cutoff = time.time() - minutes * 60
        try:
            with open(log_p, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
        except Exception:
            return Response("", mimetype="text/plain")
        # Best-effort: filter by [YYYY-MM-DD HH:MM:SS] prefix if present.
        kept = []
        for ln in lines:
            keep = True
            if ln.startswith("[") and len(ln) > 21:
                try:
                    ts = datetime.datetime.strptime(ln[1:20], "%Y-%m-%d %H:%M:%S").timestamp()
                    keep = ts >= cutoff
                except Exception:
                    pass
            if keep:
                kept.append(ln)
        # Fallback: if timestamps don't parse, just return the tail (~200KB max)
        if not kept:
            kept = lines[-2000:]
        return Response("".join(kept), mimetype="text/plain")

    # ── backup schedule toggle ───────────────────────────────────
    @app.route("/server/schedule-backup/<folder>", methods=["GET", "POST"])
    def schedule_backup(folder):
        if not auth_check():
            return jsonify({"status": "error"}), 403
        verify_folder_ownership(folder)
        if request.method == "GET":
            cfg = _backup_sched.get(folder) or {"enabled": False, "interval_h": 24}
            return jsonify(cfg)
        if not check_csrf():
            return jsonify({"status": "error", "msg": "CSRF"}), 403
        d = request.json or {}
        enabled = bool(d.get("enabled"))
        try:
            interval_h = max(1, min(168, int(d.get("interval_h", 24))))
        except Exception:
            interval_h = 24
        _backup_sched[folder] = {"enabled": enabled, "interval_h": interval_h,
                                 "last_run": _backup_sched.get(folder, {}).get("last_run", 0.0)}
        conn = _db()
        conn.execute("""INSERT INTO backup_schedule(folder, enabled, interval_h)
                        VALUES(?,?,?)
                        ON CONFLICT(folder) DO UPDATE SET
                          enabled=excluded.enabled, interval_h=excluded.interval_h""",
                     (folder, 1 if enabled else 0, interval_h))
        conn.commit(); conn.close()
        return jsonify({"status": "ok", "enabled": enabled, "interval_h": interval_h})

    # ── notify config ────────────────────────────────────────────
    @app.route("/user/notify-config", methods=["GET", "POST"])
    def notify_config():
        uid = session.get("user_id")
        if not uid:
            return jsonify({"status": "error"}), 403
        if request.method == "GET":
            conn = _db(); conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT webhook_url, email, events FROM notify_config WHERE user_id=?",
                               (uid,)).fetchone()
            conn.close()
            return jsonify(dict(row) if row else {"webhook_url": "", "email": "", "events": "crash,restart"})
        if not check_csrf():
            return jsonify({"status": "error", "msg": "CSRF"}), 403
        d = request.json or {}
        webhook = (d.get("webhook_url") or "").strip()[:500]
        email   = (d.get("email") or "").strip()[:200]
        events  = ",".join([e for e in (d.get("events") or "crash,restart").split(",")
                            if e in ("crash", "restart", "backup")])
        conn = _db()
        conn.execute("""INSERT INTO notify_config(user_id, webhook_url, email, events)
                        VALUES(?,?,?,?)
                        ON CONFLICT(user_id) DO UPDATE SET
                          webhook_url=excluded.webhook_url,
                          email=excluded.email,
                          events=excluded.events,
                          updated_at=CURRENT_TIMESTAMP""",
                     (uid, webhook, email, events))
        conn.commit(); conn.close()
        return jsonify({"status": "ok"})

    # ── multi-bot overview ───────────────────────────────────────
    @app.route("/servers/overview")
    def servers_overview():
        uid = session.get("user_id")
        if not uid:
            return jsonify({"servers": []}), 403
        conn = get_db()
        rows = conn.execute("SELECT id, name, folder FROM servers WHERE user_id=?",
                            (uid,)).fetchall()
        conn.close()
        out = []
        for r in rows:
            folder = r["folder"]
            with _ext_lock:
                hist = list(_stats_hist.get(folder, ()))
            online_samples = sum(1 for s in hist if s.get("online"))
            pct = round(100 * online_samples / len(hist), 1) if hist else 0.0
            # live check
            live = False
            try:
                import psutil
                rp = _get_running_procs(context)
                if folder in rp and rp[folder].poll() is None:
                    live = True
            except Exception:
                pass
            out.append({
                "id": r["id"], "name": r["name"], "folder": folder,
                "online": live, "uptime_pct": pct, "samples": len(hist),
            })
        return jsonify({"servers": out})

    @app.route("/overview")
    def overview_page():
        if not auth_check():
            return "Login required", 403
        return render_template_string(OVERVIEW_HTML)


def _get_running_procs(context):
    return context.get("running_procs", {})
