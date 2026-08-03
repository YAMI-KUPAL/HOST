# ─────────────────────────────────────────────
# IMPORTANT: eventlet.monkey_patch() must run before anything else
# imports socket/threading/subprocess, or the SocketIO event loop will
# block on every subprocess call / DB call / file read once this panel
# has more than one concurrent user (which is the whole point on Railway).
# ─────────────────────────────────────────────
import eventlet
eventlet.monkey_patch()

import os, sqlite3, zipfile, subprocess, signal, shutil, psutil, time, datetime, secrets, hashlib, re, json
from collections import defaultdict
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, g, abort
from werkzeug.utils import secure_filename
from flask_socketio import SocketIO, emit

# ─────────────────────────────────────────────
# Persistent data root. On Railway, mount a Volume and set DATA_DIR to its
# mount path (e.g. /data) so storage/instances (user servers) and the
# SQLite DB survive redeploys/restarts. Defaults to the working directory,
# which is fine for local dev but NOT persistent on Railway without a volume.
# ─────────────────────────────────────────────
DATA_DIR = os.environ.get('DATA_DIR', os.getcwd())

# ─────────────────────────────────────────────
# In-memory security stores
# ─────────────────────────────────────────────
running_procs = {}
start_times = {}
# Orphan bots discovered on boot: folder -> (pid, pid_start).
# These are alive processes we can't Popen-adopt but must still count as "online"
# and be able to signal via os.killpg.
orphan_pids = {}


def pgid_has_live_process(pgid):
    """True iff any process on the system currently belongs to process group
    `pgid` and is alive/not a zombie. Used as a fallback for is_folder_online()/
    resolve_live_pid() when the originally tracked leader PID has died but the
    bot forked/re-exec'd/spawned children that are still running under the
    same process group (os.killpg already treats that group as one unit on
    stop, so "online" detection should match that same unit)."""
    if not pgid:
        return False
    try:
        pgid = int(pgid)
    except (TypeError, ValueError):
        return False
    try:
        for p in psutil.process_iter(['pid']):
            try:
                if os.getpgid(p.pid) != pgid:
                    continue
                if not p.is_running() or p.status() == psutil.STATUS_ZOMBIE:
                    continue
                return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError, OSError):
                continue
    except (psutil.Error, OSError):
        return False
    return False


def process_is_ours(pid, pid_start, cmd_hint=None):
    """True iff pid is alive AND its create_time matches the stored start.
    Guards against PID reuse after a panel restart. Optionally validates that
    the process cmdline references cmd_hint (e.g. the startup file)."""
    if not pid or not pid_start:
        return False
    try:
        p = psutil.Process(int(pid))
        if abs(p.create_time() - float(pid_start)) > 1.0:
            return False
        if not p.is_running() or p.status() == psutil.STATUS_ZOMBIE:
            return False
        if cmd_hint:
            try:
                if not any(cmd_hint in part for part in p.cmdline()):
                    return False
            except psutil.Error:
                return False
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.Error, ValueError, TypeError):
        return False


def _saved_pgid(folder, saved_pgid=None):
    """Best-effort pgid lookup: prefer the explicitly saved one (from the DB
    row), fall back to the live Popen handle's pgid if we still have it."""
    if saved_pgid:
        try:
            return int(saved_pgid)
        except (TypeError, ValueError):
            pass
    proc = running_procs.get(folder)
    if proc is not None:
        try:
            return os.getpgid(proc.pid)
        except (ProcessLookupError, OSError):
            pass
    return None


def is_folder_online(folder, saved_pid, saved_pid_start, saved_pgid=None):
    """Single source of truth for 'is this server currently running?'.
    Checks the live Popen handle first, then the stored (pid, start_time),
    then the orphan-adopted table populated at boot, then — if the leader PID
    itself is dead — whether any process is still alive in the same process
    group (covers bots that fork/re-exec/spawn children under the original
    group, since os.killpg already treats that group as one unit on stop)."""
    proc = running_procs.get(folder)
    if proc is not None and proc.poll() is None:
        return True
    if process_is_ours(saved_pid, saved_pid_start):
        return True
    op = orphan_pids.get(folder)
    if op and process_is_ours(op[0], op[1]):
        return True
    pgid = _saved_pgid(folder, saved_pgid)
    if pgid and pgid_has_live_process(pgid):
        return True
    return False


def resolve_live_pid(folder, saved_pid, saved_pid_start, saved_pgid=None):
    """Return the PID currently backing this folder, or None. Falls back to
    any live member of the process group when the leader PID has died."""
    proc = running_procs.get(folder)
    if proc is not None and proc.poll() is None:
        return proc.pid
    if process_is_ours(saved_pid, saved_pid_start):
        return int(saved_pid)
    op = orphan_pids.get(folder)
    if op and process_is_ours(op[0], op[1]):
        return int(op[0])
    pgid = _saved_pgid(folder, saved_pgid)
    if pgid and pgid_has_live_process(pgid):
        try:
            for p in psutil.process_iter(['pid']):
                try:
                    if os.getpgid(p.pid) == pgid and p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                        return p.pid
                except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError, OSError):
                    continue
        except (psutil.Error, OSError):
            pass
    return None

_rate_store = defaultdict(list)          # ip -> [timestamps]
_login_attempts = defaultdict(list)      # ip -> [timestamps]
_blocked_ips = {}                        # ip -> unblock_timestamp
_captcha_required = set()               # IPs that need captcha
_clipboard = {}                         # user session clipboard for copy/cut

RATE_LIMIT = 120           # requests per minute
LOGIN_MAX_ATTEMPTS = 5     # before block
LOGIN_WINDOW = 300         # 5 min window
LOGIN_BLOCK_DURATION = 900 # 15 min block
CAPTCHA_AFTER = 3          # failed logins before captcha
SESSION_TIMEOUT = 3600     # 1 hour

socketio = SocketIO(async_mode='eventlet')

# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
def get_db():
    db_path = os.path.join(DATA_DIR, 'storage/nehost.db')
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    storage_dir = os.path.join(DATA_DIR, 'storage')
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    db = get_db()
    db.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fname TEXT, lname TEXT, username TEXT, email TEXT, password TEXT, pfp TEXT DEFAULT 'default.png',
        role TEXT DEFAULT 'free',
        status TEXT DEFAULT 'active',
        server_limit INTEGER DEFAULT 1,
        notifications TEXT DEFAULT ''
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS servers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, name TEXT, folder TEXT,
        status TEXT, startup TEXT, pid INTEGER,
        server_status TEXT DEFAULT 'active'
    )''')
    # NOTE: pid_start/pgid columns are added below via ALTER TABLE migrations
    # (wrapped in try/except) so existing SQLite files created before these
    # columns existed keep working without a manual migration step.
    db.execute('''CREATE TABLE IF NOT EXISTS tickets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, subject TEXT, message TEXT, status TEXT DEFAULT 'open',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS admin_settings (
        id INTEGER PRIMARY KEY,
        username TEXT, password TEXT,
        popup_title TEXT, popup_msg TEXT, popup_img TEXT, show_popup INTEGER DEFAULT 0
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS security_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_type TEXT, ip_address TEXT, user_id INTEGER,
        details TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS ip_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ip_address TEXT UNIQUE,
        rule_type TEXT,  -- 'block' or 'allow'
        reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    if not db.execute('SELECT * FROM admin_settings WHERE id=1').fetchone():
        db.execute('INSERT INTO admin_settings (id, username, password) VALUES (1, "YAMI", "09667664037")')
    # Schema migration: pid_start column (psutil create_time snapshot) to detect PID reuse.
    cols = {r[1] for r in db.execute('PRAGMA table_info(servers)').fetchall()}
    if 'pid_start' not in cols:
        db.execute('ALTER TABLE servers ADD COLUMN pid_start REAL')
    # Schema migration: pgid column so we can fall back to "any process still
    # alive in this process group" when the tracked leader PID has died (bot
    # forked/re-exec'd/spawned children). Wrapped in try/except so it's a
    # no-op on DBs that already have the column.
    try:
        if 'pgid' not in cols:
            db.execute('ALTER TABLE servers ADD COLUMN pgid INTEGER')
    except sqlite3.OperationalError:
        pass
    # Schema migration: description + type columns for server settings
    try:
        if 'description' not in cols:
            db.execute('ALTER TABLE servers ADD COLUMN description TEXT DEFAULT ""')
    except sqlite3.OperationalError:
        pass
    try:
        if 'type' not in cols:
            db.execute('ALTER TABLE servers ADD COLUMN type TEXT DEFAULT "python"')
    except sqlite3.OperationalError:
        pass
    db.commit()
    db.close()


# ─────────────────────────────────────────────
# Security helpers
# ─────────────────────────────────────────────
def get_real_ip():
    return request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip()

def is_ip_blocked(ip):
    if ip in _blocked_ips:
        if time.time() < _blocked_ips[ip]:
            return True
        else:
            del _blocked_ips[ip]
    db = get_db()
    rule = db.execute("SELECT rule_type FROM ip_rules WHERE ip_address=? AND rule_type='block'", (ip,)).fetchone()
    db.close()
    return rule is not None

def check_rate_limit(ip):
    now = time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < 60]
    _rate_store[ip].append(now)
    return len(_rate_store[ip]) <= RATE_LIMIT

def record_login_attempt(ip, success, user_id=None, details=''):
    db = get_db()
    db.execute('INSERT INTO security_logs (event_type, ip_address, user_id, details) VALUES (?,?,?,?)',
               ('login_success' if success else 'login_fail', ip, user_id, details))
    db.commit()
    db.close()
    if not success:
        now = time.time()
        _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < LOGIN_WINDOW]
        _login_attempts[ip].append(now)
        count = len(_login_attempts[ip])
        if count >= CAPTCHA_AFTER:
            _captcha_required.add(ip)
        if count >= LOGIN_MAX_ATTEMPTS:
            _blocked_ips[ip] = now + LOGIN_BLOCK_DURATION

def needs_captcha(ip):
    return ip in _captcha_required

def verify_captcha(answer, expected):
    try:
        return int(answer) == int(expected)
    except:
        return False

def generate_captcha_session():
    """Generate a math captcha, store expected answer in session, return the question."""
    import random
    a = random.randint(1, 20)
    b = random.randint(1, 20)
    session['captcha_expected'] = a + b
    return f"{a} + {b}"

def check_captcha_session(answer):
    """Validate captcha answer against session-stored expected value."""
    try:
        result = int(answer) == session.get('captcha_expected', -1)
        session.pop('captcha_expected', None)  # single-use
        return result
    except:
        return False

def generate_csrf_token():
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(32)
    return session['csrf_token']

def check_csrf():
    token = request.form.get('csrf_token') or request.headers.get('X-CSRF-Token') or (request.json or {}).get('csrf_token')
    return token == session.get('csrf_token')

def sanitize(s, max_len=255):
    if s is None:
        return ''
    s = str(s)[:max_len]
    s = re.sub(r'[<>"\';&|`$]', '', s)
    return s.strip()

def check_session_timeout():
    last = session.get('last_active', 0)
    if time.time() - last > SESSION_TIMEOUT:
        session.clear()
        return False
    session['last_active'] = time.time()
    return True

def log_admin_action(action, details=''):
    ip = get_real_ip()
    db = get_db()
    db.execute('INSERT INTO security_logs (event_type, ip_address, user_id, details) VALUES (?,?,?,?)',
               (action, ip, session.get('user_id'), details))
    db.commit()
    db.close()

def safe_path(base, *parts):
    """Build a path and ensure it stays within base using realpath canonicalization."""
    base_real = os.path.realpath(base)
    cleaned_parts = []
    for p in parts:
        if p:
            # Strip leading slashes to prevent resetting path join to system root
            p_clean = p.lstrip('/\\')
            if p_clean:
                cleaned_parts.append(p_clean)
    joined = os.path.realpath(os.path.join(base_real, *cleaned_parts))
    try:
        os.path.commonpath([base_real, joined])  # raises ValueError on different drives
    except ValueError:
        abort(403)
    if not joined.startswith(base_real + os.sep) and joined != base_real:
        abort(403)
    return joined

# ─────────────────────────────────────────────
# App factory
# ─────────────────────────────────────────────
def create_app():
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates"))
    app.config['SECRET_KEY'] = os.environ.get('SESSION_SECRET', secrets.token_hex(32))
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    # Railway terminates TLS in front of the container, so cookies can be
    # marked Secure in that environment. Left off for plain local dev.
    app.config['SESSION_COOKIE_SECURE'] = os.environ.get('RAILWAY_ENVIRONMENT') is not None
    app.config['BASE_STORAGE'] = os.path.join(DATA_DIR, 'storage/instances')
    app.config['UPLOAD_FOLDER'] = os.path.join(DATA_DIR, 'static/uploads')

    for p in [app.config['BASE_STORAGE'], app.config['UPLOAD_FOLDER']]:
        os.makedirs(p, exist_ok=True)

    init_db()
    # Lock this down to your Railway domain via CORS_ORIGINS once you know it
    # (e.g. "https://yourapp.up.railway.app"). Comma-separate multiple origins.
    _cors = os.environ.get('CORS_ORIGINS', '*')
    socketio.init_app(app, cors_allowed_origins=_cors.split(',') if _cors != '*' else '*')

    # ── Security middleware ──────────────────
    @app.before_request
    def security_checks():
        ip = get_real_ip()
        # Skip static files
        if request.endpoint == 'static':
            return
        # Rate limit
        if not check_rate_limit(ip):
            return jsonify({'status': 'error', 'msg': 'Rate limit exceeded. Slow down.'}), 429
        # IP block check
        if is_ip_blocked(ip) and request.endpoint not in ('admin_login',):
            return jsonify({'status': 'error', 'msg': 'Your IP is blocked.'}), 403
        # Session timeout for authenticated routes
        if session.get('user_id') or session.get('admin_logged'):
            if not check_session_timeout():
                if request.is_json:
                    return jsonify({'status': 'error', 'msg': 'Session expired'}), 401
                return redirect(url_for('login'))

    @app.after_request
    def set_security_headers(resp):
        resp.headers['X-Content-Type-Options'] = 'nosniff'
        resp.headers['X-Frame-Options'] = 'SAMEORIGIN'
        resp.headers['X-XSS-Protection'] = '1; mode=block'
        resp.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        return resp

    # ── Uptime helper ────────────────────────
    def get_precise_uptime(start_timestamp):
        if not start_timestamp:
            return "Offline"
        diff = int(time.time() - start_timestamp)
        months, rem = divmod(diff, 2592000)
        days, rem = divmod(rem, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)
        parts = []
        if months > 0: parts.append(f"{months}mo")
        if days > 0: parts.append(f"{days}d")
        if hours > 0: parts.append(f"{hours}h")
        if minutes > 0: parts.append(f"{minutes}m")
        parts.append(f"{secs}s")
        return " ".join(parts)

    # ── Context processors ───────────────────
    @app.context_processor
    def inject_csrf():
        return dict(csrf_token=generate_csrf_token())

    # ── Ownership enforcement ─────────────────
    def verify_folder_ownership(folder):
        """Abort 403 if the folder does not belong to the current user (or admin)."""
        if session.get('admin_logged'):
            return  # admin can access any folder
        uid = session.get('user_id')
        if not uid:
            abort(403)
        db = get_db()
        srv = db.execute('SELECT user_id FROM servers WHERE folder=?', (folder,)).fetchone()
        db.close()
        if not srv or int(srv['user_id']) != int(uid):
            abort(403)

    # ─────────────────────────────────────────
    # Public routes
    # ─────────────────────────────────────────
    @app.route('/')
    def home():
        return render_template('index.html')

    @app.route('/signup', methods=['GET', 'POST'])
    def signup():
        if request.method == 'POST':
            fname = sanitize(request.form.get('fname', ''), 60)
            lname = sanitize(request.form.get('lname', ''), 60)
            username = sanitize(request.form.get('username', ''), 60)
            email = sanitize(request.form.get('email', ''), 120)
            pwd = request.form.get('password', '')
            cpwd = request.form.get('confirm_password', '')

            if not re.match(r'^[\w.+-]+@[\w-]+\.[a-z]{2,}$', email, re.I):
                return jsonify({'status': 'error', 'msg': 'Invalid email format'}), 400
            if len(pwd) < 6:
                return jsonify({'status': 'error', 'msg': 'Password must be at least 6 characters'}), 400
            if pwd != cpwd:
                return jsonify({'status': 'error', 'msg': 'Passwords do not match!'}), 400

            db = get_db()
            existing = db.execute('SELECT id FROM users WHERE email=? OR username=?', (email, username)).fetchone()
            if existing:
                db.close()
                return jsonify({'status': 'error', 'msg': 'Email or Username already taken!'}), 400

            pfp_name = 'default.png'
            pfp = request.files.get('pfp')
            if pfp and pfp.filename:
                ext = os.path.splitext(pfp.filename)[1].lower()
                if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                    db.close()
                    return jsonify({'status': 'error', 'msg': 'Invalid image type'}), 400
                pfp_name = secure_filename(pfp.filename)
                pfp.save(os.path.join(app.config['UPLOAD_FOLDER'], pfp_name))

            db.execute('''INSERT INTO users (fname, lname, username, email, password, pfp, server_limit, role, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (fname, lname, username, email, pwd, pfp_name, 1, 'free', 'active'))
            db.commit()
            db.close()
            return jsonify({'status': 'success', 'url': url_for('login')})
        return render_template('web/signup.html')

    @app.route('/captcha/generate')
    def captcha_generate():
        """Return a new captcha question, storing expected answer server-side in the session."""
        question = generate_captcha_session()
        return jsonify({'question': question})

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        ip = get_real_ip()
        if request.method == 'POST':
            if is_ip_blocked(ip):
                return jsonify({'status': 'error', 'msg': 'Too many failed attempts. Try again in 15 minutes.'}), 429

            email = sanitize(request.form.get('email', ''), 120)
            pwd = request.form.get('password', '')

            # CAPTCHA check — answer validated against session value (not client-supplied expected)
            if needs_captcha(ip):
                cap_answer = request.form.get('captcha_answer', '')
                if not check_captcha_session(cap_answer):
                    # regenerate for next attempt
                    generate_captcha_session()
                    return jsonify({'status': 'captcha_fail', 'msg': 'CAPTCHA verification failed. Try again.', 'needs_captcha': True}), 400

            db = get_db()
            user = db.execute('SELECT * FROM users WHERE (email=? OR username=?) AND password=?', (email, email, pwd)).fetchone()
            db.close()

            if user:
                if user['status'] == 'banned':
                    record_login_attempt(ip, False, details=f'Banned user attempt: {email}')
                    return jsonify({'status': 'banned', 'msg': 'Your account is suspended!'}), 403
                session['user_id'] = user['id']
                session['last_active'] = time.time()
                session.permanent = False
                _captcha_required.discard(ip)
                _login_attempts.pop(ip, None)
                record_login_attempt(ip, True, user_id=user['id'], details=f'User: {email}')
                return jsonify({'status': 'success', 'url': url_for('dashboard')}), 200
            else:
                record_login_attempt(ip, False, details=f'Failed: {email}')
                attempts_left = max(0, LOGIN_MAX_ATTEMPTS - len(_login_attempts[ip]))
                cap_needed = needs_captcha(ip)
                resp = {
                    'status': 'error',
                    'msg': f'Invalid credentials! {attempts_left} attempts remaining.',
                    'needs_captcha': cap_needed
                }
                if cap_needed:
                    # Pre-generate a fresh captcha question for the next attempt
                    resp['captcha_question'] = generate_captcha_session()
                return jsonify(resp), 401
        return render_template('web/login.html')

    @app.route('/logout')
    def logout():
        session.clear()
        return redirect(url_for('login'))

    # ─────────────────────────────────────────
    # Dashboard
    # ─────────────────────────────────────────
    @app.route('/dashboard')
    def dashboard():
        if 'user_id' not in session:
            return redirect(url_for('login'))
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
        db.close()
        if not user or user['status'] != 'active':
            session.clear()
            return redirect(url_for('login'))
        notif = user['notifications'] or ''
        return render_template('web/dashboard.html', user=user, notification=notif)

    @app.route('/profile/update', methods=['POST'])
    def update_profile():
        if 'user_id' not in session:
            return jsonify({'status': 'error'})
        uid = session['user_id']
        fname = sanitize(request.form.get('fname', ''), 60)
        lname = sanitize(request.form.get('lname', ''), 60)
        pwd = request.form.get('password', '')
        db = get_db()
        if pwd and len(pwd) >= 6:
            db.execute('UPDATE users SET fname=?, lname=?, password=? WHERE id=?', (fname, lname, pwd, uid))
        else:
            db.execute('UPDATE users SET fname=?, lname=? WHERE id=?', (fname, lname, uid))
        db.commit()
        db.close()
        return jsonify({'status': 'ok'})

    @app.route('/ticket/create', methods=['POST'])
    def create_ticket():
        if 'user_id' not in session:
            return jsonify({'status': 'error'})
        d = request.json or {}
        subject = sanitize(d.get('subject', ''), 200)
        message = sanitize(d.get('message', ''), 2000)
        db = get_db()
        db.execute('INSERT INTO tickets (user_id, subject, message) VALUES (?,?,?)',
                   (session['user_id'], subject, message))
        db.commit()
        db.close()
        return jsonify({'status': 'ok'})

    # ─────────────────────────────────────────
    # Admin routes
    # ─────────────────────────────────────────
    @app.route('/admin-login', methods=['GET', 'POST'])
    def admin_login():
        ip = get_real_ip()
        if request.method == 'POST':
            if is_ip_blocked(ip):
                return render_template('web/admin_login.html', error='IP temporarily blocked.')
            user = sanitize(request.form.get('username', ''), 80)
            pwd = request.form.get('password', '')
            db = get_db()
            admin = db.execute('SELECT * FROM admin_settings WHERE username=? AND password=?', (user, pwd)).fetchone()
            db.close()
            if admin:
                session['admin_logged'] = True
                session['last_active'] = time.time()
                log_admin_action('admin_login', f'Admin login from {ip}')
                return redirect(url_for('admin_panel'))
            record_login_attempt(ip, False, details=f'Admin failed login: {user}')
            return render_template('web/admin_login.html', error='Invalid credentials')
        return render_template('web/admin_login.html')

    @app.route('/admin/panel')
    def admin_panel():
        if not session.get('admin_logged'):
            return redirect(url_for('admin_login'))
        return render_template('web/admin_panel.html')

    @app.route('/admin/stats')
    def admin_stats():
        if not session.get('admin_logged'):
            return jsonify({})
        db = get_db()
        users = db.execute('SELECT * FROM users').fetchall()
        user_list = []
        total_cpu = psutil.cpu_percent()
        total_ram = psutil.virtual_memory().percent
        total_disk = psutil.disk_usage('/').percent
        for u in users:
            srvs = db.execute('SELECT * FROM servers WHERE user_id=?', (u['id'],)).fetchall()
            active_srvs = 0
            for s in srvs:
                if is_folder_online(s['folder'], s['pid'], s['pid_start'] if 'pid_start' in s.keys() else None,
                                     s['pgid'] if 'pgid' in s.keys() else None):
                    active_srvs += 1

            user_list.append({
                'id': u['id'], 'fname': u['fname'], 'email': u['email'] or '',
                'srv_count': len(srvs), 'active_srvs': active_srvs,
                'status': u['status'], 'role': u['role'], 'server_limit': u['server_limit']
            })
        db.close()
        return jsonify({
            'users': user_list,
            'sys_cpu': f"{total_cpu:.1f}%",
            'sys_ram': f"{total_ram:.1f}%",
            'sys_disk': f"{total_disk:.1f}%"
        })

    @app.route('/admin/user/update', methods=['POST'])
    def update_user():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        d = request.json or {}
        db = get_db()
        db.execute('UPDATE users SET role=?, status=?, server_limit=? WHERE id=?',
                   (sanitize(d.get('role','free')), sanitize(d.get('status','active')), int(d.get('limit',1)), int(d['user_id'])))
        db.commit()
        db.close()
        log_admin_action('user_update', f"Updated user {d.get('user_id')}")
        return jsonify({'status': 'ok'})

    @app.route('/admin/set-popup', methods=['POST'])
    def set_popup():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        title = sanitize(request.form.get('title', ''), 200)
        msg = sanitize(request.form.get('msg', ''), 1000)
        show = request.form.get('show')
        img = request.files.get('image')
        db = get_db()
        old_data = db.execute('SELECT popup_img FROM admin_settings WHERE id=1').fetchone()
        img_name = old_data['popup_img'] if old_data else None
        if img and img.filename:
            img_name = secure_filename(img.filename)
            img.save(os.path.join(app.config['UPLOAD_FOLDER'], img_name))
        db.execute('UPDATE admin_settings SET popup_title=?, popup_msg=?, popup_img=?, show_popup=? WHERE id=1',
                   (title, msg, img_name, 1 if show == 'true' else 0))
        db.commit()
        db.close()
        return jsonify({'status': 'ok'})

    @app.route('/admin/send-warning', methods=['POST'])
    def send_warning():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        d = request.json or {}
        msg = sanitize(d.get('message', ''), 500)
        uid = int(d.get('user_id', 0))
        db = get_db()
        db.execute('UPDATE users SET notifications=? WHERE id=?', (msg, uid))
        db.commit()
        db.close()
        log_admin_action('send_warning', f'Warning to user {uid}')
        return jsonify({'status': 'ok'})

    @app.route('/admin/login-as/<int:uid>')
    def login_as(uid):
        if not session.get('admin_logged'):
            return redirect(url_for('admin_login'))
        session['user_id'] = uid
        session['last_active'] = time.time()
        log_admin_action('login_as', f'Admin logged in as user {uid}')
        return redirect(url_for('dashboard'))

    @app.route('/admin/manage-user/<int:uid>')
    def admin_manage_user_servers(uid):
        if not session.get('admin_logged'):
            return redirect(url_for('admin_login'))
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE id=?', (uid,)).fetchone()
        rows = db.execute('SELECT * FROM servers WHERE user_id=?', (uid,)).fetchall()
        db.close()
        servers = []
        for r in rows:
            f = r['folder']
            online = is_folder_online(f, r['pid'], r['pid_start'] if 'pid_start' in r.keys() else None,
                                       r['pgid'] if 'pgid' in r.keys() else None)
            servers.append({'id': r['id'], 'name': r['name'], 'folder': f, 'online': online, 'status': r['server_status']})

        return render_template('web/admin_manage_user.html', user=user, servers=servers)

    @app.route('/admin/suspend-server/<int:sid>', methods=['POST'])
    def admin_suspend_server(sid):
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        status = sanitize((request.json or {}).get('status', 'suspended'))
        db = get_db()
        db.execute('UPDATE servers SET server_status=? WHERE id=?', (status, sid))
        db.commit()
        db.close()
        log_admin_action('suspend_server', f'Server {sid} -> {status}')
        return jsonify({'status': 'ok'})

    @app.route('/admin/delete-server/<int:sid>', methods=['POST'])
    def admin_delete_server(sid):
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        db = get_db()
        srv = db.execute('SELECT folder FROM servers WHERE id=?', (sid,)).fetchone()
        if srv:
            folder = srv['folder']
            # Kill live proc or orphan
            for _pid in (running_procs[folder].pid if folder in running_procs else None,
                        orphan_pids.get(folder, (None, None))[0]):
                if _pid:
                    try: os.killpg(os.getpgid(_pid), signal.SIGKILL)
                    except: pass
            running_procs.pop(folder, None)
            orphan_pids.pop(folder, None)

            db.execute('DELETE FROM servers WHERE id=?', (sid,))
            db.commit()
            path = os.path.join(app.config['BASE_STORAGE'], folder)
            if os.path.exists(path):
                shutil.rmtree(path)
            db.close()
            log_admin_action('delete_server', f'Deleted server {sid}')
            return jsonify({'status': 'deleted'})
        db.close()
        return jsonify({'status': 'error', 'msg': 'Server not found'})

    @app.route('/admin/create-user', methods=['POST'])
    def admin_create_user():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        d = request.json or {}
        db = get_db()
        db.execute('INSERT INTO users (fname, email, password, server_limit) VALUES (?,?,?,?)',
                   (sanitize(d.get('name','')), sanitize(d.get('email','')), d.get('pass',''), int(d.get('limit',1))))
        db.commit()
        db.close()
        log_admin_action('create_user', f"Created user {d.get('email')}")
        return jsonify({'status': 'ok'})

    @app.route('/admin/delete-user/<int:uid>', methods=['POST'])
    def delete_user(uid):
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        db = get_db()
        srvs = db.execute('SELECT folder FROM servers WHERE user_id=?', (uid,)).fetchall()
        for s in srvs:
            path = os.path.join(app.config['BASE_STORAGE'], s['folder'])
            if os.path.exists(path):
                shutil.rmtree(path)
        db.execute('DELETE FROM servers WHERE user_id=?', (uid,))
        db.execute('DELETE FROM users WHERE id=?', (uid,))
        db.commit()
        db.close()
        log_admin_action('delete_user', f'Deleted user {uid}')
        return jsonify({'status': 'deleted'})

    @app.route('/admin/files/<folder>')
    def admin_browse_files(folder):
        if not session.get('admin_logged'):
            return redirect(url_for('admin_login'))
        return render_template('web/dashboard.html', user={'fname': 'Admin'}, is_admin_view=True, admin_folder=folder)

    # ─────────────────────────────────────────
    # Security admin routes
    # ─────────────────────────────────────────
    @app.route('/admin/security/logs')
    def security_logs():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        db = get_db()
        logs = db.execute('SELECT * FROM security_logs ORDER BY created_at DESC LIMIT 200').fetchall()
        db.close()
        return jsonify({'logs': [dict(l) for l in logs]})

    @app.route('/admin/security/ip-rules')
    def get_ip_rules():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        db = get_db()
        rules = db.execute('SELECT * FROM ip_rules ORDER BY created_at DESC').fetchall()
        db.close()
        return jsonify({'rules': [dict(r) for r in rules]})

    @app.route('/admin/security/block-ip', methods=['POST'])
    def block_ip():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        d = request.json or {}
        ip = sanitize(d.get('ip', ''), 50)
        reason = sanitize(d.get('reason', ''), 200)
        if not ip:
            return jsonify({'status': 'error', 'msg': 'IP required'})
        db = get_db()
        db.execute('INSERT OR REPLACE INTO ip_rules (ip_address, rule_type, reason) VALUES (?,?,?)', (ip, 'block', reason))
        db.commit()
        db.close()
        log_admin_action('block_ip', f'Blocked IP {ip}: {reason}')
        return jsonify({'status': 'ok'})

    @app.route('/admin/security/unblock-ip', methods=['POST'])
    def unblock_ip():
        if not session.get('admin_logged'):
            return jsonify({'status': 'error'})
        ip = sanitize((request.json or {}).get('ip', ''), 50)
        db = get_db()
        db.execute("DELETE FROM ip_rules WHERE ip_address=? AND rule_type='block'", (ip,))
        db.commit()
        db.close()
        _blocked_ips.pop(ip, None)
        log_admin_action('unblock_ip', f'Unblocked IP {ip}')
        return jsonify({'status': 'ok'})

    # ─────────────────────────────────────────
    # File manager routes (enhanced)
    # ─────────────────────────────────────────
    def auth_check():
        return 'user_id' in session or session.get('admin_logged')

    @app.route('/files/list/<folder>')
    def flist(folder):
        if not auth_check(): return jsonify([])
        verify_folder_ownership(folder)
        sub_path = request.args.get('path', '')
        full_path = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        if not os.path.exists(full_path): return jsonify([])
        items = []
        for f in sorted(os.listdir(full_path), key=lambda x: (not os.path.isdir(os.path.join(full_path, x)), x.lower())):
            if f == 'console.log': continue
            p = os.path.join(full_path, f)
            stat = os.stat(p)
            ext = os.path.splitext(f)[1].lower().lstrip('.')
            items.append({
                'name': f,
                'is_dir': os.path.isdir(p),
                'is_zip': f.lower().endswith('.zip'),
                'rel_path': os.path.join(sub_path, f),
                'size': stat.st_size,
                'size_human': _human_size(stat.st_size),
                'modified': datetime.datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M'),
                'ext': ext,
                'permissions': oct(stat.st_mode)[-3:]
            })
        return jsonify(items)

    def _human_size(b):
        for unit in ['B','KB','MB','GB']:
            if b < 1024: return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    @app.route('/files/read/<folder>')
    def fread(folder):
        if not auth_check(): return jsonify({'content': 'Unauthorized'})
        verify_folder_ownership(folder)
        name = request.args.get('name', '')
        sub_path = request.args.get('path', '')
        p = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        try:
            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                return jsonify({'content': f.read()})
        except:
            return jsonify({'content': 'Error reading file'})

    @app.route('/files/content/<folder>/<name>')
    def fcontent(folder, name):
        if not auth_check(): return jsonify({'content': 'Unauthorized'})
        sub_path = request.args.get('path', '')
        p = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        try:
            with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                return jsonify({'content': f.read()})
        except:
            return jsonify({'content': 'Error reading file'})

    @app.route('/files/save/<folder>', methods=['POST'])
    def fsave_query(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        name = request.args.get('name', '') or (request.json or {}).get('name', '')
        sub_path = request.args.get('path', '') or (request.json or {}).get('path', '')
        content = (request.json or {}).get('content', '')
        p = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        try:
            with open(p, 'w', encoding='utf-8') as f: f.write(content)
            return jsonify({'status': 'saved'})
        except:
            return jsonify({'status': 'error'})

    @app.route('/files/save/<folder>/<name>', methods=['POST'])
    def fsave(folder, name):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        sub_path = request.args.get('path', '')
        p = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        try:
            with open(p, 'w', encoding='utf-8') as f: f.write((request.json or {}).get('content', ''))
            return jsonify({'status': 'saved'})
        except:
            return jsonify({'status': 'error'})

    @app.route('/files/delete-bulk/<folder>', methods=['POST'])
    def delete_bulk(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        sub_path, names = d.get('path', ''), d.get('names', [])
        base = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        if not names:
            names = [f for f in os.listdir(base) if f != 'console.log']
        for name in names:
            if name == 'console.log': continue
            p = safe_path(base, name)
            try:
                if os.path.isdir(p): shutil.rmtree(p)
                elif os.path.exists(p): os.remove(p)
            except: pass
        return jsonify({"status": "ok"})

    @app.route('/files/create-file/<folder>', methods=['POST'])
    def create_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        name = secure_filename(d.get('name', 'newfile.txt'))
        p = safe_path(app.config['BASE_STORAGE'], folder, d.get('path', ''), name)
        with open(p, 'w') as f: f.write("")
        return jsonify({'status': 'ok'})

    @app.route('/files/create-folder/<folder>', methods=['POST'])
    def create_folder(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        name = secure_filename(d.get('name', 'newfolder'))
        p = safe_path(app.config['BASE_STORAGE'], folder, d.get('path', ''), name)
        os.makedirs(p, exist_ok=True)
        return jsonify({'status': 'ok'})

    @app.route('/files/upload/<folder>', methods=['POST'])
    def upload_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        sub_path = request.form.get('path', '')
        file = request.files.get('file')
        if not file: return jsonify({'status': 'error', 'msg': 'No file provided'})
        dest = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        os.makedirs(dest, exist_ok=True)
        file.save(os.path.join(dest, secure_filename(file.filename)))
        return jsonify({'status': 'ok'})

    @app.route('/files/rename/<folder>', methods=['POST'])
    def rename_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        base = safe_path(app.config['BASE_STORAGE'], folder, d.get('path', ''))
        old_path = safe_path(base, d.get('old', ''))
        new_path = safe_path(base, secure_filename(d.get('new', '')))
        os.rename(old_path, new_path)
        return jsonify({'status': 'ok'})

    @app.route('/files/copy/<folder>', methods=['POST'])
    def copy_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        sub_path = d.get('path', '')
        name = d.get('name', '')
        src = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        base_dir = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        # Store in clipboard
        uid = session.get('user_id', 'admin')
        _clipboard[uid] = {'src': src, 'name': name, 'action': d.get('action', 'copy')}
        return jsonify({'status': 'ok'})

    @app.route('/files/paste/<folder>', methods=['POST'])
    def paste_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        uid = session.get('user_id', 'admin')
        if uid not in _clipboard:
            return jsonify({'status': 'error', 'msg': 'Nothing in clipboard'})
        clip = _clipboard[uid]
        sub_path = (request.json or {}).get('path', '')
        dest_dir = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        dest = os.path.join(dest_dir, os.path.basename(clip['name']))
        src = clip['src']
        try:
            if os.path.isdir(src):
                shutil.copytree(src, dest)
            else:
                shutil.copy2(src, dest)
            if clip['action'] == 'cut':
                if os.path.isdir(src): shutil.rmtree(src)
                else: os.remove(src)
                del _clipboard[uid]
        except Exception as e:
            return jsonify({'status': 'error', 'msg': str(e)})
        return jsonify({'status': 'ok'})

    @app.route('/files/duplicate/<folder>', methods=['POST'])
    def duplicate_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        sub_path = d.get('path', '')
        name = d.get('name', '')
        src = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        base_dir = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        stem, ext = os.path.splitext(name)
        dest_name = f"{stem}_copy{ext}"
        dest = os.path.join(base_dir, dest_name)
        i = 1
        while os.path.exists(dest):
            dest_name = f"{stem}_copy{i}{ext}"
            dest = os.path.join(base_dir, dest_name)
            i += 1
        try:
            if os.path.isdir(src): shutil.copytree(src, dest)
            else: shutil.copy2(src, dest)
        except Exception as e:
            return jsonify({'status': 'error', 'msg': str(e)})
        return jsonify({'status': 'success', 'new_name': dest_name})

    @app.route('/files/download/<folder>/<name>')
    def download_file(folder, name):
        if not auth_check(): abort(403)
        verify_folder_ownership(folder)
        sub_path = request.args.get('path', '')
        p = safe_path(app.config['BASE_STORAGE'], folder, sub_path, name)
        return send_file(p, as_attachment=True)

    @app.route('/files/download-folder/<folder>')
    def download_folder(folder):
        if not auth_check(): abort(403)
        verify_folder_ownership(folder)
        sub_path = request.args.get('path', '')
        folder_name = request.args.get('name', '')
        src = safe_path(app.config['BASE_STORAGE'], folder, sub_path, folder_name)
        if not os.path.isdir(src):
            return jsonify({'status': 'error', 'msg': 'Not a folder'}), 400
        import tempfile
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
        with zipfile.ZipFile(tmp.name, 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(src):
                for file in files:
                    fp = os.path.join(root, file)
                    zf.write(fp, os.path.relpath(fp, os.path.dirname(src)))
        return send_file(tmp.name, as_attachment=True, download_name=f"{folder_name}.zip")

    @app.route('/files/zip-bulk/<folder>', methods=['POST'])
    def zip_bulk(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        names, sub_path = d.get('names', []), d.get('path', '')
        base = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        if not names:
            names = [f for f in os.listdir(base) if f != 'console.log']
        zip_name = f"archive_{int(time.time())}.zip"
        zip_path = os.path.join(base, zip_name)
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
            for n in names:
                p = safe_path(base, n)
                if n == zip_name: continue
                if os.path.isdir(p):
                    for root, dirs, files in os.walk(p):
                        for file in files:
                            full_p = os.path.join(root, file)
                            z.write(full_p, os.path.relpath(full_p, base))
                elif os.path.exists(p):
                    z.write(p, n)
        return jsonify({'status': 'success', 'zip': zip_name})

    @app.route('/files/unzip/<folder>', methods=['POST'])
    def unzip_file(folder):
        if not auth_check(): return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        zip_name = d.get('name')
        sub_path = d.get('path', '')
        base = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        zip_path = os.path.join(base, zip_name)
        if os.path.exists(zip_path) and zipfile.is_zipfile(zip_path):
            try:
                with zipfile.ZipFile(zip_path, 'r') as z:
                    z.extractall(base)
                return jsonify({'status': 'ok'})
            except Exception as e:
                return jsonify({'status': 'error', 'msg': str(e)})
        return jsonify({'status': 'error', 'msg': 'Invalid zip file'})

    @app.route('/files/search/<folder>')
    def search_files(folder):
        if not auth_check(): return jsonify([])
        verify_folder_ownership(folder)
        query = request.args.get('q', '').lower()
        sub_path = request.args.get('path', '')
        base = safe_path(app.config['BASE_STORAGE'], folder, sub_path)
        results = []
        for root, dirs, files in os.walk(base):
            for name in dirs + files:
                if query in name.lower():
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, base)
                    results.append({'name': name, 'path': rel, 'is_dir': os.path.isdir(full)})
                    if len(results) >= 50: break
            if len(results) >= 50: break
        return jsonify(results)

    # ─────────────────────────────────────────
    # Server control routes
    # ─────────────────────────────────────────
    @app.route('/server/action/<folder>/<act>', methods=['POST'])
    def server_action(folder, act):
        if not auth_check(): return jsonify({'status': 'error', 'msg': 'Unauthorized'}), 403
        verify_folder_ownership(folder)
        db = get_db()
        srv_data = db.execute('SELECT server_status FROM servers WHERE folder=?', (folder,)).fetchone()
        if srv_data and srv_data['server_status'] == 'suspended':
            db.close()
            return jsonify({'status': 'error', 'msg': 'This server is suspended by Admin.'})
        path = os.path.join(app.config['BASE_STORAGE'], folder)
        log_file_path = os.path.join(path, 'console.log')
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if act == 'install':
            req_path = os.path.join(path, 'requirements.txt')
            if os.path.exists(req_path):
                f_log = open(log_file_path, 'a')
                f_log.write(f"\n[{now}] [INFO] Package Installation Started...\n")
                f_log.flush()
                subprocess.Popen(['pip', 'install', '-r', 'requirements.txt'], cwd=path, stdout=f_log, stderr=f_log)
                db.close()
                return jsonify({'status': 'installing'})
            db.close()
            return jsonify({'status': 'error', 'msg': 'requirements.txt missing'})

        if act in ['start', 'restart']:
            row = db.execute('SELECT pid, pid_start FROM servers WHERE folder=?', (folder,)).fetchone()
            old_pid = row['pid'] if row else None
            old_pid_start = row['pid_start'] if row else None
            # Kill any live proc, matching-start-time PID, OR orphan tracked at boot.
            for _pid in filter(None, [
                running_procs[folder].pid if folder in running_procs else None,
                old_pid if process_is_ours(old_pid, old_pid_start) else None,
                orphan_pids.get(folder, (None, None))[0],
            ]):
                try: os.killpg(os.getpgid(_pid), signal.SIGKILL)
                except: pass
            running_procs.pop(folder, None)
            orphan_pids.pop(folder, None)
            srv = db.execute('SELECT startup FROM servers WHERE folder=?', (folder,)).fetchone()
            startup_file = srv['startup'] if srv and srv['startup'] else 'main.py'
            f_log = open(log_file_path, 'a')
            f_log.write(f"\n[{now}] [INFO] Instance {act.upper()}ED Successfully\n")
            proc = subprocess.Popen(['python3', startup_file], cwd=path, stdout=f_log, stderr=f_log, preexec_fn=os.setsid)
            try:
                pstart = psutil.Process(proc.pid).create_time()
            except psutil.Error:
                pstart = time.time()
            try:
                pgid = os.getpgid(proc.pid)
            except OSError:
                pgid = None
            running_procs[folder], start_times[folder] = proc, pstart
            db.execute('UPDATE servers SET pid=?, pid_start=?, pgid=? WHERE folder=?', (proc.pid, pstart, pgid, folder))
            db.commit()
            db.close()
            return jsonify({'status': 'started'})

        elif act == 'stop':
            row = db.execute('SELECT pid, pid_start FROM servers WHERE folder=?', (folder,)).fetchone()
            saved_pid = row['pid'] if row else None
            saved_start = row['pid_start'] if row else None
            for _pid in filter(None, [
                running_procs[folder].pid if folder in running_procs else None,
                saved_pid if process_is_ours(saved_pid, saved_start) else None,
                orphan_pids.get(folder, (None, None))[0],
            ]):
                try: os.killpg(os.getpgid(_pid), signal.SIGKILL)
                except: pass
            running_procs.pop(folder, None)
            orphan_pids.pop(folder, None)
            db.execute('UPDATE servers SET pid=NULL, pid_start=NULL, pgid=NULL WHERE folder=?', (folder,))
            db.commit()
            db.close()
            with open(log_file_path, 'a') as f:
                f.write(f"\n[{now}] [INFO] Instance STOPPED\n")
            return jsonify({'status': 'stopped'})

        db.close()
        return jsonify({'status': 'ok'})

    @app.route('/server/command/<folder>', methods=['POST'])
    def server_command(folder):
        if not auth_check():
            return jsonify({'status': 'error'})
        verify_folder_ownership(folder)
        d = request.json or {}
        cmd = d.get('command', '')
        path = os.path.join(app.config['BASE_STORAGE'], folder)
        log_file_path = os.path.join(path, 'console.log')
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            result = subprocess.run(cmd, shell=True, cwd=path, capture_output=True, text=True, timeout=15)
            output = result.stdout + result.stderr
            with open(log_file_path, 'a') as f:
                f.write(f"\n[{now}] [CMD] $ {cmd}\n{output}\n")
            return jsonify({'status': 'ok', 'output': output})
        except subprocess.TimeoutExpired:
            return jsonify({'status': 'error', 'msg': 'Command timed out'})
        except Exception as e:
            return jsonify({'status': 'error', 'msg': str(e)})

    @app.route('/server/log/<folder>')
    def server_log(folder):
        """Delta-based log streaming: only return bytes after `offset` to avoid re-sending the full file.

        `tail=1` (or an omitted `offset`) means "prime the terminal with the
        freshest output" — instead of starting at byte 0 of a potentially huge
        file, seek to the last ~32KB so a Start/Restart shows current output
        immediately rather than requiring the client to page forward from the
        beginning. Explicit forward paging via `offset` (offset > 0) is
        unchanged and still caps each read at 128KB."""
        if not auth_check(): return jsonify({'log': '', 'size': 0}), 403
        verify_folder_ownership(folder)
        path = os.path.join(app.config['BASE_STORAGE'], folder, 'console.log')
        has_offset = 'offset' in request.args
        offset = max(0, int(request.args.get('offset', 0)))
        want_tail = request.args.get('tail') == '1' or not has_offset
        try:
            # Binary mode: offsets are byte-accurate, no newline-translation drift
            with open(path, 'rb') as f:
                f.seek(0, 2)
                size = f.tell()
                if want_tail:
                    offset = max(0, size - 32768)
                    f.seek(offset)
                    chunk_bytes = f.read()
                else:
                    # File was truncated/rotated → reset to beginning
                    if offset > size:
                        offset = 0
                    f.seek(offset)
                    chunk_bytes = f.read(131072)  # max 128 KB per poll
            new_offset = offset + len(chunk_bytes)
            chunk = chunk_bytes.decode('utf-8', errors='replace')
            return jsonify({'log': chunk, 'offset': new_offset, 'size': size})
        except FileNotFoundError:
            return jsonify({'log': '', 'offset': 0, 'size': 0})
        except Exception as e:
            return jsonify({'log': '', 'offset': offset, 'size': 0})

    @app.route('/server/install/<folder>', methods=['POST'])
    def install_package(folder):
        """Pip-install a package inside the server's working directory.
        Requires user confirmation on the frontend before calling this endpoint.
        Command injection is prevented by strict regex validation of the package name.
        CSRF-validated to prevent cross-site triggering."""
        if not auth_check(): return jsonify({'status': 'error', 'msg': 'Unauthorized'}), 403
        if not check_csrf(): return jsonify({'status': 'error', 'msg': 'CSRF validation failed'}), 403
        verify_folder_ownership(folder)
        raw_pkg = (request.json or {}).get('package', '').strip()
        pkg = sanitize(raw_pkg, 120)
        # Only allow safe pip package specifier characters
        if not pkg or not re.fullmatch(r'[a-zA-Z0-9_\-\.\[\]>=<!,\s]+', pkg):
            return jsonify({'status': 'error', 'msg': 'Invalid package name'}), 400
        srv_path = safe_path(app.config['BASE_STORAGE'], folder)
        log_path = os.path.join(srv_path, 'console.log')
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            with open(log_path, 'a') as f:
                f.write(f'\n[{now}] [INSTALL] Running: pip install --user {pkg}\n')
            result = subprocess.run(
                ['pip', 'install', '--user', pkg],
                capture_output=True, text=True, timeout=120, cwd=srv_path
            )
            out = (result.stdout + result.stderr)[:4000]
            success = result.returncode == 0
            with open(log_path, 'a') as f:
                f.write(f'{out}\n[{now}] [INSTALL] {"SUCCESS" if success else "FAILED"}: pip install {pkg}\n')
            log_admin_action('install_package', f'folder={folder} pkg={pkg} ok={success}')
            return jsonify({'status': 'success' if success else 'error', 'output': out})
        except subprocess.TimeoutExpired:
            return jsonify({'status': 'error', 'msg': 'Installation timed out (120 s)'}), 408
        except Exception as e:
            return jsonify({'status': 'error', 'msg': str(e)}), 500

    @app.route('/server/stats/<folder>')
    def server_stats(folder):
        if not auth_check(): return jsonify({}), 403
        verify_folder_ownership(folder)
        db = get_db()
        srv = db.execute('SELECT name, description, type FROM servers WHERE folder=?', (folder,)).fetchone()
        db.close()
        db = get_db()
        row = db.execute('SELECT pid, pid_start, pgid, startup FROM servers WHERE folder=?', (folder,)).fetchone()
        db.close()
        saved_pid = row['pid'] if row else None
        saved_start = row['pid_start'] if row else None
        saved_pgid = row['pgid'] if row and 'pgid' in row.keys() else None
        startup = row['startup'] if row else 'main.py'
        online = False
        cpu, ram, threads, pid = '0', '0', '0', '-'
        uptime = 'Offline'

        p_pid = resolve_live_pid(folder, saved_pid, saved_start, saved_pgid)
        if p_pid:
            try:
                proc = psutil.Process(p_pid)
                if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                    online = True
                    cpu = f"{proc.cpu_percent(interval=0.1):.1f}"
                    ram = f"{proc.memory_info().rss / (1024*1024):.1f}"
                    threads = str(proc.num_threads())
                    pid = str(p_pid)
                    ref_start = start_times.get(folder) or saved_start or (orphan_pids.get(folder, (None, None))[1])
                    uptime = get_precise_uptime(ref_start) if ref_start else 'Online'
            except: pass


        sys_cpu = psutil.cpu_percent()
        sys_ram = psutil.virtual_memory()
        sys_disk = psutil.disk_usage('/')
        net = psutil.net_io_counters()

        return jsonify({
            'online': online,
            'cpu': cpu, 'mem': ram, 'disk': '0', 'threads': threads,
            'pid': pid, 'uptime': uptime, 'startup': startup,
            'name': srv['name'] if srv else folder,
            'desc': srv['description'] if srv else '',
            'type': srv['type'] if srv else 'python',
            'reqfile': 'requirements.txt',
            'sys_cpu': f"{sys_cpu:.1f}",
            'sys_ram_used': f"{sys_ram.used / (1024**3):.1f}",
            'sys_ram_total': f"{sys_ram.total / (1024**3):.1f}",
            'sys_ram_pct': f"{sys_ram.percent:.1f}",
            'sys_disk_used': f"{sys_disk.used / (1024**3):.1f}",
            'sys_disk_total': f"{sys_disk.total / (1024**3):.1f}",
            'sys_disk_pct': f"{sys_disk.percent:.1f}",
            'net_sent': f"{net.bytes_sent / (1024**2):.1f}",
            'net_recv': f"{net.bytes_recv / (1024**2):.1f}"
        })

    @app.route('/server/set-startup/<folder>', methods=['POST'])
    def set_startup(folder):
        if not auth_check(): return jsonify({'status': 'error'}), 403
        verify_folder_ownership(folder)
        data = request.json or {}
        db = get_db()
        if 'file' in data or 'startup' in data:
            cmd = data.get('file') or data.get('startup')
            db.execute('UPDATE servers SET startup=? WHERE folder=?', (cmd, folder))
        if 'name' in data:
            db.execute('UPDATE servers SET name=? WHERE folder=?', (data['name'], folder))
        if 'desc' in data:
            db.execute('UPDATE servers SET description=? WHERE folder=?', (data['desc'], folder))
        db.commit()
        db.close()
        return jsonify({'status': 'ok'})

    @app.route('/server/delete/<folder>', methods=['POST'])
    def delete_server(folder):
        if not auth_check(): return jsonify({'status': 'error'}), 403
        verify_folder_ownership(folder)
        data = request.json or {}
        action = data.get('action', 'delete')
        db = get_db()
        srv_data = db.execute('SELECT server_status, pid, pid_start FROM servers WHERE folder=?', (folder,)).fetchone()
        if srv_data and srv_data['server_status'] == 'suspended':
            db.close()
            return jsonify({'status': 'error', 'msg': 'Suspended servers cannot be deleted!'})
        # Kill running process
        for _pid in filter(None, [
            running_procs[folder].pid if folder in running_procs else None,
            srv_data['pid'] if srv_data and process_is_ours(srv_data['pid'], srv_data['pid_start']) else None,
            orphan_pids.get(folder, (None, None))[0],
        ]):
            try: os.killpg(os.getpgid(_pid), signal.SIGKILL)
            except: pass
        running_procs.pop(folder, None)
        orphan_pids.pop(folder, None)

        if action == 'delete':
            db.execute('DELETE FROM servers WHERE folder=?', (folder,))
            db.commit()
            db.close()
            path = os.path.join(app.config['BASE_STORAGE'], folder)
            if os.path.exists(path): shutil.rmtree(path)
            return jsonify({'status': 'ok'})
        elif action == 'rebuild':
            db.close()
            # Reinstall requirements on next start
            return jsonify({'status': 'ok'})
        else:
            db.close()
            return jsonify({'status': 'ok'})

    @app.route('/servers')
    def list_servers():
        if 'user_id' not in session: return jsonify({'servers': []})
        db = get_db()
        user = db.execute('SELECT username FROM users WHERE id=?', (session['user_id'],)).fetchone()
        rows = db.execute('SELECT * FROM servers WHERE user_id=?', (session['user_id'],)).fetchall()
        db.close()
        srvs = []
        for r in rows:
            f, saved_pid = r['folder'], r['pid']
            saved_start = r['pid_start'] if 'pid_start' in r.keys() else None
            saved_pgid = r['pgid'] if 'pgid' in r.keys() else None
            p_pid = resolve_live_pid(f, saved_pid, saved_start, saved_pgid)
            online = p_pid is not None
            ref_start = start_times.get(f) or saved_start or (orphan_pids.get(f, (None, None))[1])
            uptime = get_precise_uptime(ref_start) if online and ref_start else ("Online" if online else "Offline")
            cpu, ram = "0%", "0MB"
            if online:
                try:
                    process = psutil.Process(p_pid)
                    cpu = f"{process.cpu_percent(interval=None):.1f}%"
                    ram = f"{process.memory_info().rss / (1024*1024):.1f}MB"
                except: pass

            srvs.append({
                'name': r['name'], 'folder': f, 'online': online,
                'startup': r['startup'] or 'main.py', 'uptime': uptime,
                'cpu': cpu, 'ram': ram, 'status': r['server_status'],
                'type': r['type'] if 'type' in r.keys() else 'python'
            })
        resp = {'servers': srvs}
        if user:
            resp['username'] = user['username']
            resp['status'] = 'ok'
        return jsonify(resp)

    @app.route('/add', methods=['POST'])
    def add_srv():
        if 'user_id' not in session: return jsonify({'status': 'error'})
        db = get_db()
        user = db.execute('SELECT * FROM users WHERE id=?', (session['user_id'],)).fetchone()
        count = db.execute('SELECT COUNT(*) as count FROM servers WHERE user_id=?', (session['user_id'],)).fetchone()['count']
        if count >= user['server_limit']:
            db.close()
            return jsonify({'status': 'error', 'msg': f"Limit Reached! Max: {user['server_limit']}"})
        name = sanitize((request.json or {}).get('name', 'server'), 80)
        folder = re.sub(r'[^a-z0-9_]', '', secure_filename(name).lower()) + "_" + str(int(time.time()))
        srv_type = sanitize((request.json or {}).get('type', 'python'), 30)
        db.execute('INSERT INTO servers (user_id, name, folder, status, startup, type) VALUES (?,?,?,?,?,?)',
                   (session['user_id'], name, folder, 'Offline', 'main.py', srv_type))
        db.commit()
        db.close()
        os.makedirs(os.path.join(app.config['BASE_STORAGE'], folder), exist_ok=True)
        return jsonify({'status': 'ok'})

    # ─────────────────────────────────────────
    # Register Pro Panel enhancements (additive, non-breaking)
    # ─────────────────────────────────────────
    try:
        from enhancements import register as _register_enhancements
        _register_enhancements(app, socketio, {
            'get_db': get_db,
            'running_procs': running_procs,
            'start_times': start_times,
            'orphan_pids': orphan_pids,
            'process_is_ours': process_is_ours,
            'pgid_has_live_process': pgid_has_live_process,
            'is_folder_online': is_folder_online,
            'resolve_live_pid': resolve_live_pid,
            'safe_path': safe_path,
            'auth_check': auth_check,
            'verify_folder_ownership': verify_folder_ownership,
            'check_csrf': check_csrf,
            'log_admin_action': log_admin_action,
            'sanitize': sanitize,
            'BASE_STORAGE': app.config['BASE_STORAGE'],
        })
    except Exception as _e:
        # Never let an enhancement failure take the panel down.
        app.logger.warning(f"Pro Panel enhancements not loaded: {_e}")

    # Extension layer (uptime history, webhooks, backup scheduler, overview).
    try:
        from enhancements_ext import register_ext as _register_ext
        _register_ext(app, socketio, {
            'get_db': get_db,
            'running_procs': running_procs,
            'start_times': start_times,
            'auth_check': auth_check,
            'verify_folder_ownership': verify_folder_ownership,
            'check_csrf': check_csrf,
            'BASE_STORAGE': app.config['BASE_STORAGE'],
        })
    except Exception as _e:
        app.logger.warning(f"Pro Panel ext not loaded: {_e}")

    return app


app = create_app()


def reconcile_running_state():
    """On boot, walk every server row with a stored PID and decide whether the
    process is still ours (start_time match) → adopt as orphan; otherwise clear
    the stale DB pointer. Handles both crash-survivor bots and PID reuse."""
    adopted = cleared = 0
    with app.app_context():
        db = get_db()
        try:
            rows = db.execute("SELECT folder, pid, pid_start FROM servers WHERE pid IS NOT NULL").fetchall()
        except sqlite3.OperationalError:
            rows = []
        for r in rows:
            folder, pid, pstart = r['folder'], r['pid'], r['pid_start']
            if pstart is None and pid:
                # legacy row from before this migration — infer from live process if PID matches; else drop.
                try:
                    pstart_live = psutil.Process(int(pid)).create_time()
                    db.execute("UPDATE servers SET pid_start=? WHERE folder=?", (pstart_live, folder))
                    pstart = pstart_live
                except psutil.Error:
                    db.execute("UPDATE servers SET pid=NULL, pid_start=NULL WHERE folder=?", (folder,))
                    cleared += 1
                    continue
            if process_is_ours(pid, pstart):
                orphan_pids[folder] = (int(pid), float(pstart))
                start_times[folder] = float(pstart)
                adopted += 1
            else:
                db.execute("UPDATE servers SET pid=NULL, pid_start=NULL WHERE folder=?", (folder,))
                cleared += 1
        db.commit()
        db.close()
    app.logger.warning(f"reconcile: {adopted} orphan(s) alive, {cleared} stale row(s) cleared")


def _shutdown_all(*_args):
    """Graceful shutdown: terminate every tracked child (live Popen + adopted
    orphans) then clear DB pointers so next boot starts clean.
    Policy: child bots do NOT survive a clean panel shutdown. Orphan survival
    is only used as a crash-recovery fallback via reconcile_running_state()."""
    pids = []
    for folder, proc in list(running_procs.items()):
        try: pids.append(proc.pid)
        except Exception: pass
    for folder, (pid, _s) in list(orphan_pids.items()):
        pids.append(pid)
    for pid in pids:
        try: os.killpg(os.getpgid(pid), signal.SIGTERM)
        except Exception: pass
    time.sleep(1.5)
    for pid in pids:
        try: os.killpg(os.getpgid(pid), signal.SIGKILL)
        except Exception: pass
    try:
        with app.app_context():
            db = get_db()
            db.execute("UPDATE servers SET pid=NULL, pid_start=NULL")
            db.commit()
            db.close()
    except Exception:
        pass


import atexit
atexit.register(_shutdown_all)
try:
    signal.signal(signal.SIGTERM, lambda *a: (_shutdown_all(), os._exit(0)))
except (ValueError, OSError):
    pass  # not on main thread (e.g. under some WSGI hosts)

reconcile_running_state()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    # allow_unsafe_werkzeug lets the dev server run on PythonAnywhere-style hosts
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)

