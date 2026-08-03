/* =========================================================
   CLAUD Pro Panel — mobile-first frontend
   - Real-time terminal (Socket.IO + polling fallback)
   - Live CPU/RAM/Disk stats
   - Process manager, AI fixer, backups, auto-restart
   - Bottom-nav tabs, swipe-to-close, skeleton loading
   All rendered inside a floating bottom sheet — the existing
   dashboard markup is untouched.
   ========================================================= */
(function () {
  'use strict';

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const html = (strings, ...vals) => {
    const t = document.createElement('template');
    t.innerHTML = String.raw(strings, ...vals).trim();
    return t.content.firstElementChild;
  };
  const esc = (s) => String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

  // Minimal ANSI → HTML (colors + reset). Enough for python coloured tracebacks.
  const ANSI = {
    30: 'color:#6b7280', 31: 'color:#ff4d5e', 32: 'color:#22e07a',
    33: 'color:#ffb020', 34: 'color:#7ec8ff', 35: 'color:#c084fc',
    36: 'color:#00e5ff', 37: 'color:#e5e7eb', 90: 'color:#4b5563',
    91: 'color:#ff8ba0', 92: 'color:#7ff0b0', 93: 'color:#ffd166',
    94: 'color:#93c5fd', 95: 'color:#e0b1ff', 96: 'color:#88f7ff',
    1: 'font-weight:700', 4: 'text-decoration:underline'
  };
  function ansiToHtml(text) {
    text = esc(text);
    let open = 0, out = '';
    text = text.replace(/\x1b\[([0-9;]*)m/g, (_, codes) => {
      let seg = '';
      const parts = codes.split(';').filter(Boolean);
      if (parts.length === 0 || parts.includes('0')) {
        seg += '</span>'.repeat(open); open = 0; return seg;
      }
      const styles = parts.map(p => ANSI[+p]).filter(Boolean).join(';');
      if (styles) { open++; seg += `<span style="${styles}">`; }
      return seg;
    });
    out = text + '</span>'.repeat(open);
    // Highlight common tokens
    out = out.replace(/(Traceback[^<]*)/g, '<span class="err">$1</span>')
             .replace(/((?:^|\n)[^\n]*Error:[^\n]*)/g, '<span class="err">$1</span>')
             .replace(/((?:^|\n)\[[^\]]+\] \[INFO\][^\n]*)/g, '<span class="info">$1</span>')
             .replace(/((?:^|\n)\[[^\]]+\] \[CMD\][^\n]*)/g, '<span class="hl">$1</span>');
    return out;
  }

  // Detect current server folder from existing dashboard state.
  function detectFolder() {
    if (window.__panelActiveFolder) return window.__panelActiveFolder;
    if (window.currentFolder) return window.currentFolder;
    if (window.activeFolder) return window.activeFolder;
    if (window.selectedFolder) return window.selectedFolder;
    if (window.curSrv && window.curSrv.folder) return window.curSrv.folder;
    const el = document.querySelector('[data-server-folder]');
    if (el) return el.getAttribute('data-server-folder');
    const p = new URLSearchParams(location.search);
    return p.get('folder') || p.get('server') || '';
  }
  let _subscribedFolder = null;
  function setActiveFolder(folder) {
    window.__panelActiveFolder = folder || null;
    ensureSocket((s) => {
      if (!s) return;
      if (_subscribedFolder && _subscribedFolder !== folder) {
        try { s.emit('term:unsubscribe', { folder: _subscribedFolder }); } catch (_) {}
      }
      if (folder && folder !== _subscribedFolder) {
        try { s.emit('term:subscribe', { folder }); s.emit('stats:subscribe', { folder }); } catch (_) {}
      }
      _subscribedFolder = folder || null;
    });
  }


  // CSRF from any existing input the app renders
  function csrfToken() {
    const el = document.querySelector('input[name="csrf_token"]');
    return el ? el.value : (window.CSRF_TOKEN || '');
  }

  async function api(path, opts = {}) {
    opts.headers = Object.assign({ 'X-CSRF-Token': csrfToken() }, opts.headers || {});
    if (opts.json !== undefined) {
      opts.body = JSON.stringify(opts.json);
      opts.headers['Content-Type'] = 'application/json';
      delete opts.json;
    }
    const r = await fetch(path, opts);
    if (r.status === 204) return {};
    try { return await r.json(); } catch { return {}; }
  }

  // ─── Socket.IO (Socket primary, polling fallback) ─────────────
  // Transport is 'socket' when the socket is connected, otherwise 'polling'.
  // Per-tab polling timers are ONLY started when transport === 'polling'.
  let socket = null;
  let transport = 'polling'; // pessimistic until socket 'connect' fires
  const transportListeners = new Set();
  function setTransport(t) {
    if (t === transport) return;
    transport = t;
    window.__panelTransport = t;
    window.dispatchEvent(new CustomEvent('panel:transport', { detail: { transport: t } }));
    transportListeners.forEach(fn => { try { fn(t); } catch (_) {} }); 
  }
  function onTransport(fn) { transportListeners.add(fn); fn(transport); return () => transportListeners.delete(fn); }

  function _wireSocket(s) {
    if (!s) return;
    s.on('connect', () => setTransport('socket'));
    s.on('disconnect', () => setTransport('polling'));
    s.on('connect_error', () => setTransport('polling'));
    // Global status broadcast (fires every ~7s from server).
    s.on('servers:status', (payload) => {
      window.__panelStatus = payload;
      window.dispatchEvent(new CustomEvent('panel:status', { detail: payload }));
    });
    // Ask the server for our per-user status room.
    try { s.emit('status:subscribe'); } catch (_) {}
  }
  function ensureSocket(cb) {
    if (socket) return cb(socket);
    if (typeof io === 'undefined') {
      const s = document.createElement('script');
      s.src = 'https://cdn.socket.io/4.7.5/socket.io.min.js';
      s.onload = () => { socket = io({ transports: ['websocket', 'polling'] }); _wireSocket(socket); cb(socket); };
      s.onerror = () => { setTransport('polling'); cb(null); };
      document.head.appendChild(s);
    } else {
      socket = io({ transports: ['websocket', 'polling'] });
      _wireSocket(socket);
      cb(socket);
    }
  }
  window.__panelReady = true;


  // ─── Panel scaffold ────────────────────────────────────
  const TABS = [
    { id: 'stats', label: 'Live',    icon: 'fa-gauge-high' },
    { id: 'term',  label: 'Console', icon: 'fa-terminal' },
    { id: 'proc',  label: 'Procs',   icon: 'fa-microchip' },
    { id: 'ai',    label: 'AI Fix',  icon: 'fa-wand-magic-sparkles' },
    { id: 'more',  label: 'More',    icon: 'fa-toolbox' }
  ];

  function buildSheet() {
    const sheet = html`
      <div class="pp-sheet" id="pp-sheet" role="dialog" aria-modal="true" aria-label="Pro Panel">
        <div class="pp-sheet-inner">
          <div class="pp-grabber"></div>
          <div class="pp-header">
            <div class="pp-title"><span class="pp-dot"></span> Pro Panel <span class="pp-muted" id="pp-folder"></span></div>
            <button class="pp-close" id="pp-close" aria-label="Close"><i class="fas fa-xmark"></i></button>
          </div>
          <div class="pp-tabs" id="pp-tabs"></div>
          <div class="pp-body" id="pp-body"></div>
        </div>
      </div>`;
    document.body.appendChild(sheet);

    const tabs = $('#pp-tabs');
    TABS.forEach((t, i) => {
      const b = html`<button class="pp-tab ${i===0?'active':''}" data-tab="${t.id}">
        <i class="fas ${t.icon}"></i><span>${t.label}</span></button>`;
      b.addEventListener('click', () => selectTab(t.id));
      tabs.appendChild(b);
    });

    $('#pp-close').addEventListener('click', closePanel);
    sheet.addEventListener('click', (e) => { if (e.target === sheet) closePanel(); });

    // Swipe-down to dismiss
    let startY = null;
    sheet.addEventListener('touchstart', (e) => { startY = e.touches[0].clientY; }, { passive: true });
    sheet.addEventListener('touchmove', (e) => {
      if (startY == null) return;
      const dy = e.touches[0].clientY - startY;
      if (dy > 90) { closePanel(); startY = null; }
    }, { passive: true });
  }

  function buildFab() {
    const fab = html`<button class="pp-fab" id="pp-fab" aria-label="Open Pro Panel">
      <i class="fas fa-bolt"></i></button>`;
    fab.addEventListener('click', openPanel);
    document.body.appendChild(fab);
  }

  function openPanel() {
    const folder = detectFolder();
    if (!folder) {
      // If no server context, still open with stats-only view
      $('#pp-folder').textContent = '(no server selected)';
    } else {
      $('#pp-folder').textContent = '· ' + folder;
    }
    $('#pp-sheet').classList.add('open');
    selectTab('stats');
    ensureSocket((s) => {
      if (!s || !folder) return;
      s.emit('term:subscribe', { folder });
      s.emit('stats:subscribe', { folder });
    });
  }
  function closePanel() {
    $('#pp-sheet').classList.remove('open');
    stopStatsPolling();
    stopTermPolling();
  }

  // ─── Tab renderers ─────────────────────────────────────
  let currentTab = null;
  function selectTab(id) {
    currentTab = id;
    $$('.pp-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === id));
    const body = $('#pp-body');
    body.innerHTML = '';
    if (id === 'stats') renderStats(body);
    else if (id === 'term') renderTerm(body);
    else if (id === 'proc') renderProc(body);
    else if (id === 'ai')   renderAI(body);
    else if (id === 'more') renderMore(body);
  }

  // ── Stats tab
  let statsTimer = null;
  function stopStatsPolling() { if (statsTimer) { clearInterval(statsTimer); statsTimer = null; } }
  function renderStats(body) {
    body.appendChild(html`
      <div class="pp-card">
        <h4>Server <span class="pp-pill" id="pp-status">…</span></h4>
        <div class="pp-stat-grid">
          <div class="pp-stat"><div class="k">CPU</div><div class="v" id="pp-s-cpu">–<small>%</small></div><div class="pp-bar"><i id="pp-b-cpu" style="width:0"></i></div></div>
          <div class="pp-stat"><div class="k">RAM</div><div class="v" id="pp-s-ram">–<small>MB</small></div><div class="pp-bar"><i id="pp-b-ram" style="width:0"></i></div></div>
          <div class="pp-stat"><div class="k">Threads</div><div class="v" id="pp-s-th">–</div></div>
          <div class="pp-stat"><div class="k">Uptime</div><div class="v" id="pp-s-up">–</div></div>
        </div>
      </div>
      <div class="pp-card">
        <h4>System</h4>
        <div class="pp-stat-grid">
          <div class="pp-stat"><div class="k">System CPU</div><div class="v" id="pp-ss-cpu">–<small>%</small></div><div class="pp-bar"><i id="pp-bb-cpu" style="width:0"></i></div></div>
          <div class="pp-stat"><div class="k">System RAM</div><div class="v" id="pp-ss-ram">–<small>%</small></div><div class="pp-bar"><i id="pp-bb-ram" style="width:0"></i></div></div>
          <div class="pp-stat"><div class="k">Disk</div><div class="v" id="pp-ss-disk">–<small>%</small></div><div class="pp-bar"><i id="pp-bb-disk" style="width:0"></i></div></div>
          <div class="pp-stat"><div class="k">Network ↑↓ MB</div><div class="v" id="pp-ss-net">– / –</div></div>
        </div>
      </div>
      <div class="pp-card">
        <h4>Quick actions</h4>
        <div class="pp-term-toolbar">
          <button class="pp-btn primary" data-act="start"><i class="fas fa-play"></i> Start</button>
          <button class="pp-btn" data-act="restart"><i class="fas fa-rotate"></i> Restart</button>
          <button class="pp-btn danger" data-act="stop"><i class="fas fa-stop"></i> Stop</button>
          <button class="pp-btn danger" id="pp-killall"><i class="fas fa-skull"></i> Kill</button>
        </div>
      </div>`);

    body.querySelectorAll('[data-act]').forEach(b => b.addEventListener('click', async () => {
      const f = detectFolder(); if (!f) return;
      b.disabled = true;
      const r = await api(`/server/action/${f}/${b.dataset.act}`, { method: 'POST' });
      b.disabled = false;
      // Both 'start' and 'restart' resolve with {status:'started'} on success;
      // re-prime the terminal (if it's mounted for this folder) from the
      // fresh tail so new output shows up immediately after the action.
      if (r && r.status === 'started' && (b.dataset.act === 'start' || b.dataset.act === 'restart')) {
        reprimeTermIfLoaded(f);
      }
    }));
    $('#pp-killall').addEventListener('click', async () => {
      const f = detectFolder(); if (!f) return;
      const r = await api(`/server/processes/${f}`);
      if (r.root_pid) await api(`/server/kill-pid/${f}`, { method: 'POST', json: { pid: r.root_pid } });
    });

    ensureSocket((s) => {
      if (s) s.on('stats:data', (p) => { applyStats(p); dispatchStats(p); });
    });
    // Prime once so UI isn't blank, then rely on socket. If no socket, poll.
    tickStats();
    onTransport((t) => {
      if (t === 'socket') {
        if (statsTimer) { clearInterval(statsTimer); statsTimer = null; }
      } else if (!statsTimer) {
        statsTimer = setInterval(tickStats, 3000);
      }
    });
  }
  function dispatchStats(p) {
    if (currentTab !== 'stats' && currentTab !== 'term') return;
    window.__panelLastStats = p;
    window.dispatchEvent(new CustomEvent('panel:stats', { detail: p }));
  }
  async function tickStats(force) {
    // Bail immediately unless the result is actually needed: the 'stats' tab
    // renders it directly, the 'term' tab shows the live badge off it, and
    // `force` allows a one-off prime (e.g. right after opening term) even if
    // called from outside those tabs' own render functions.
    if (!force && currentTab !== 'stats' && currentTab !== 'term') return;
    const f = detectFolder(); if (!f) return;
    const d = await api(`/server/stats/${f}`).catch(() => null);
    if (!d) return;
    const p = {
      online: d.online, cpu: parseFloat(d.cpu||0), ram_mb: parseFloat(d.ram||0),
      threads: parseInt(d.threads||0,10), uptime_s: null,
      pid: d.pid,
      sys: {
        cpu: parseFloat(d.sys_cpu||0), ram_pct: parseFloat(d.sys_ram_pct||0),
        disk_pct: parseFloat(d.sys_disk_pct||0),
        net_sent_mb: parseFloat(d.net_sent||0), net_recv_mb: parseFloat(d.net_recv||0),
        ram_used_gb: parseFloat(d.sys_ram_used||0), ram_total_gb: parseFloat(d.sys_ram_total||0),
        disk_used_gb: parseFloat(d.sys_disk_used||0), disk_total_gb: parseFloat(d.sys_disk_total||0),
      }, _uptime_txt: d.uptime, _raw: d
    };
    applyStats(p);
    dispatchStats(p);
  }

  // Stats are only rendered on the 'stats' tab, but the live status badge on
  // the 'term' tab also needs them (via the panel:stats event) — so both
  // tabs are allowed through; anything else (proc/ai/more) bails immediately
  // to avoid needless work from the ~1/s socket push while parked elsewhere.
  function applyStats(p) {
    if (currentTab !== 'stats') return;
    // Batch all DOM writes into a single rAF to avoid layout thrash when
    // several stats ticks land in the same frame (e.g. socket burst).
    requestAnimationFrame(() => {
      if (currentTab !== 'stats') return; // tab may have changed by the time the frame runs
      const set = (id, v) => { const el = $('#'+id); if (el) el.innerHTML = v; };
      const bar = (id, pct) => { const el = $('#'+id); if (el) el.style.width = Math.min(100, pct||0) + '%'; };
      const status = $('#pp-status'); if (status) {
        status.textContent = p.online ? 'Online' : 'Offline';
        status.className = 'pp-pill ' + (p.online ? 'on' : 'off');
      }
      set('pp-s-cpu', `${(p.cpu||0).toFixed(1)}<small>%</small>`); bar('pp-b-cpu', p.cpu||0);
      set('pp-s-ram', `${(p.ram_mb||0).toFixed(1)}<small>MB</small>`); bar('pp-b-ram', Math.min(100, (p.ram_mb||0)/5));
      set('pp-s-th', p.threads ?? '–');
      let up = p._uptime_txt;
      if (!up && p.uptime_s != null) {
        const s = p.uptime_s; const h=Math.floor(s/3600), m=Math.floor(s%3600/60), sec=s%60;
        up = (h?h+'h ':'') + (m?m+'m ':'') + sec+'s';
      }
      set('pp-s-up', up || (p.online ? 'Online' : 'Offline'));
      if (p.sys) {
        set('pp-ss-cpu', `${(p.sys.cpu||0).toFixed(1)}<small>%</small>`); bar('pp-bb-cpu', p.sys.cpu);
        set('pp-ss-ram', `${(p.sys.ram_pct||0).toFixed(1)}<small>%</small>`); bar('pp-bb-ram', p.sys.ram_pct);
        set('pp-ss-disk', `${(p.sys.disk_pct||0).toFixed(1)}<small>%</small>`); bar('pp-bb-disk', p.sys.disk_pct);
        set('pp-ss-net', `${(p.sys.net_sent_mb||0).toFixed(1)} / ${(p.sys.net_recv_mb||0).toFixed(1)}`);
      }
    });
  }

  // ── Terminal tab
  let autoScroll = true, termPollTimer = null, termOffset = 0, cmdHistory = [], histIdx = -1;
  let termLoadedFolder = null;      // folder the currently-mounted terminal was primed for
  let termEarliestOffset = null;    // byte offset of the oldest content currently shown (for "Load earlier")
  let termByteLen = 0;              // running byte count of the live-streamed ring buffer
  const TERM_RING_MAX_BYTES = 200 * 1024; // ~200KB ring buffer, matches the "prune before append" fix
  let termPending = [];             // chunks queued for the next rAF flush
  let termFlushScheduled = false;
  function stopTermPolling() { if (termPollTimer) { clearInterval(termPollTimer); termPollTimer = null; } }
  function byteLen(s) { return new TextEncoder().encode(s || '').length; }

  function updateLiveBadge(p) {
    const dot = $('#pp-term-live .dot');
    const txt = $('#pp-term-live .txt');
    if (!dot || !txt) return;
    const online = !!(p && p.online);
    dot.className = 'dot ' + (online ? 'on' : 'off');
    txt.textContent = online ? 'Online' : 'Offline';
  }

  function renderTerm(body) {
    body.appendChild(html`
      <div class="pp-card pp-term-wrap">
        <div class="pp-term-toolbar">
          <span class="pp-pill" id="pp-term-live" title="Live status"><i class="dot"></i><span class="txt">…</span></span>
          <input class="pp-input" id="pp-term-search" placeholder="Search logs…" style="flex:1; min-width:120px;"/>
          <button class="pp-btn small" id="pp-term-clear"><i class="fas fa-eraser"></i></button>
          <button class="pp-btn small" id="pp-term-dl"><i class="fas fa-download"></i></button>
          <button class="pp-btn small" id="pp-term-scroll" title="Auto-scroll"><i class="fas fa-angles-down"></i></button>
          <button class="pp-btn small" id="pp-term-full"><i class="fas fa-expand"></i></button>
        </div>
        <div class="pp-term-toolbar">
          <button class="pp-btn small" id="pp-term-earlier" style="flex:1"><i class="fas fa-clock-rotate-left"></i> Load earlier output</button>
        </div>
        <div class="pp-term" id="pp-term" tabindex="0"></div>
        <div class="pp-input-row">
          <input class="pp-input" id="pp-cmd" placeholder="$ command  (Enter to run · ↑/↓ history)" autocomplete="off"/>
          <button class="pp-btn primary" id="pp-cmd-run"><i class="fas fa-paper-plane"></i></button>
        </div>
      </div>
      <div id="pp-diag-inline"></div>`);

    termOffset = 0;
    termEarliestOffset = null;
    termByteLen = 0;
    termPending = [];
    termFlushScheduled = false;
    const term = $('#pp-term');
    const search = $('#pp-term-search');
    const earlierBtn = $('#pp-term-earlier');
    earlierBtn.disabled = true;

    // Prime the live badge immediately (same one-off pattern as tickStats()
    // priming the stats tab) and keep it in sync via the existing
    // 'panel:stats' event bus — no new polling loop is introduced.
    if (window.__panelLastStats) updateLiveBadge(window.__panelLastStats);
    window.addEventListener('panel:stats', (e) => updateLiveBadge(e.detail));
    tickStats(true);

    $('#pp-term-clear').addEventListener('click', () => {
      term.innerHTML = '';
      termByteLen = 0;
    });
    $('#pp-term-dl').addEventListener('click', () => {
      const f = detectFolder(); if (!f) return;
      const blob = new Blob([term.innerText], { type: 'text/plain' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `${f}-console.log`; a.click();
    });
    $('#pp-term-scroll').addEventListener('click', (e) => {
      autoScroll = !autoScroll;
      e.currentTarget.style.color = autoScroll ? 'var(--pp-cyan)' : 'var(--pp-dim)';
    });
    $('#pp-term-full').addEventListener('click', () => term.classList.toggle('full'));

    // Debounce search input ~150ms and batch the resulting DOM writes into a
    // single rAF so typing doesn't thrash layout against a large log.
    let searchDebounceTimer = null;
    search.addEventListener('input', () => {
      clearTimeout(searchDebounceTimer);
      searchDebounceTimer = setTimeout(() => {
        const q = search.value.toLowerCase();
        const spans = $$('#pp-term span[data-line]');
        requestAnimationFrame(() => {
          spans.forEach(el => {
            el.style.display = !q || el.textContent.toLowerCase().includes(q) ? '' : 'none';
          });
        });
      }, 150);
    });

    earlierBtn.addEventListener('click', () => loadEarlier(earlierBtn));

    const cmd = $('#pp-cmd');
    cmd.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') { runCmd(cmd.value); cmd.value = ''; }
      else if (e.key === 'ArrowUp') { if (histIdx < cmdHistory.length - 1) { histIdx++; cmd.value = cmdHistory[cmdHistory.length-1-histIdx] || ''; } e.preventDefault(); }
      else if (e.key === 'ArrowDown') { if (histIdx > 0) { histIdx--; cmd.value = cmdHistory[cmdHistory.length-1-histIdx] || ''; } else { histIdx=-1; cmd.value=''; } e.preventDefault(); }
    });
    $('#pp-cmd-run').addEventListener('click', () => { runCmd(cmd.value); cmd.value=''; });

    // Live via socket; poll only when transport === 'polling'.
    ensureSocket((s) => {
      if (s) {
        s.on('term:data', (p) => {
          if (p.folder !== detectFolder()) return;
          appendChunk(p.chunk);
          dispatchLog(p.chunk);
        });
        s.on('term:diagnosis', (p) => { if (p.folder === detectFolder()) inlineDiag(p.diagnosis); });
      }
    });
    // Prime from the tail of the log (last ~32KB) instead of offset=0, so a
    // freshly Start/Restart-ed server shows current output immediately
    // rather than requiring the client to page forward from byte 0.
    (async () => {
      const f = detectFolder(); if (!f) return;
      termLoadedFolder = f;
      const r = await api(`/server/log/${f}?tail=1`);
      if (r) {
        if (r.log) { appendChunk(r.log); dispatchLog(r.log); }
        termOffset = r.offset || 0;
        termEarliestOffset = Math.max(0, (r.offset || 0) - byteLen(r.log || ''));
        earlierBtn.disabled = termEarliestOffset <= 0;
      }
    })();
    onTransport((t) => {
      if (t === 'socket') {
        if (termPollTimer) { clearInterval(termPollTimer); termPollTimer = null; }
      } else if (!termPollTimer) {
        termPollTimer = setInterval(async () => {
          const f = detectFolder(); if (!f) return;
          const r = await api(`/server/log/${f}?offset=${termOffset}`);
          if (r && r.log) { appendChunk(r.log); dispatchLog(r.log); termOffset = r.offset || termOffset; }
        }, 2500);
      }
    });
  }
  function dispatchLog(chunk) {
    window.dispatchEvent(new CustomEvent('panel:log', { detail: { folder: detectFolder(), chunk } }));
  }

  // Re-prime the terminal from the fresh tail after a successful Start/Restart,
  // so new output shows up immediately instead of waiting for forward paging
  // to catch up (or looking "stuck" on stale pre-restart content).
  async function reprimeTermIfLoaded(folder) {
    if (termLoadedFolder !== folder) return;
    const term = $('#pp-term'); if (!term) return;
    const r = await api(`/server/log/${folder}?tail=1`);
    if (!r) return;
    term.innerHTML = '';
    termByteLen = 0;
    termOffset = r.offset || 0;
    if (r.log) { appendChunk(r.log); dispatchLog(r.log); }
    termEarliestOffset = Math.max(0, (r.offset || 0) - byteLen(r.log || ''));
    const earlierBtn = $('#pp-term-earlier');
    if (earlierBtn) earlierBtn.disabled = termEarliestOffset <= 0;
  }

  async function loadEarlier(btn) {
    const f = detectFolder(); if (!f || termEarliestOffset == null || termEarliestOffset <= 0) return;
    btn.disabled = true;
    const start = Math.max(0, termEarliestOffset - 32768);
    const r = await api(`/server/log/${f}?offset=${start}`).catch(() => null);
    if (r && r.log) {
      // The forward-paging read may overlap content we've already shown;
      // trim that overlap off the end before prepending to avoid duplicates.
      const totalBytes = byteLen(r.log);
      const overlapBytes = Math.max(0, (start + totalBytes) - termEarliestOffset);
      let text = r.log;
      if (overlapBytes > 0 && overlapBytes < totalBytes) {
        const bytes = new TextEncoder().encode(text);
        text = new TextDecoder('utf-8', { fatal: false }).decode(bytes.slice(0, bytes.length - overlapBytes));
      }
      prependChunk(text);
      termEarliestOffset = start;
    }
    btn.disabled = termEarliestOffset == null || termEarliestOffset <= 0;
  }

  function prependChunk(text) {
    const term = $('#pp-term'); if (!term || !text) return;
    const wrap = document.createElement('span');
    wrap.setAttribute('data-line', '1');
    wrap.innerHTML = ansiToHtml(text);
    const prevHeight = term.scrollHeight;
    term.insertBefore(wrap, term.firstChild);
    // Preserve scroll position so loading history doesn't jump the view.
    // Manually-loaded older history is intentionally exempt from the live
    // ring-buffer prune below — it's an explicit, on-demand action.
    term.scrollTop += (term.scrollHeight - prevHeight);
  }

  // Batches appendChunk() calls into a single rAF per frame, prunes the
  // terminal to a ~200KB ring buffer *before* growing it further, and only
  // auto-scrolls when the user was already near the bottom (within 50px) —
  // fixes laggy scrolling from unconditional scrollTop writes + an
  // unbounded/oversized DOM pruned only by node count.
  function appendChunk(chunk) {
    if (!chunk) return;
    termPending.push(chunk);
    if (termFlushScheduled) return;
    termFlushScheduled = true;
    requestAnimationFrame(flushTermChunks);
  }

  function flushTermChunks() {
    termFlushScheduled = false;
    const term = $('#pp-term');
    if (!term) { termPending = []; return; }
    if (!termPending.length) return;
    // Single sync layout read per frame (not per chunk) to decide auto-scroll.
    const wasNearBottom = (term.scrollHeight - term.scrollTop - term.clientHeight) < 50;
    const frag = document.createDocumentFragment();
    termPending.forEach((text) => {
      const bytes = text.length;
      const wrap = document.createElement('span');
      wrap.setAttribute('data-line', '1');
      wrap.dataset.bytes = String(bytes);
      wrap.innerHTML = ansiToHtml(text);
      frag.appendChild(wrap);
      termByteLen += bytes;
    });
    termPending = [];
    // Ring-buffer prune BEFORE the fragment is visible would require knowing
    // its size ahead of time; since we already tallied termByteLen above,
    // prune immediately after appending so the buffer never sits far over
    // budget, replacing the old childNodes.length > 400 node-count heuristic
    // with an actual ~200KB byte budget.
    term.appendChild(frag);
    while (termByteLen > TERM_RING_MAX_BYTES && term.childNodes.length > 1) {
      const removed = term.firstChild;
      termByteLen -= parseInt(removed.dataset.bytes || '0', 10);
      term.removeChild(removed);
    }
    if (autoScroll && wasNearBottom) term.scrollTop = term.scrollHeight;
  }
  async function runCmd(cmd) {
    cmd = (cmd || '').trim(); if (!cmd) return;
    const f = detectFolder(); if (!f) return;
    cmdHistory.push(cmd); histIdx = -1;
    appendChunk(`\n\x1b[36m$ ${cmd}\x1b[0m\n`);
    const r = await api(`/server/command/${f}`, { method: 'POST', json: { command: cmd } });
    if (r && r.output) appendChunk(r.output + '\n');
  }
  function inlineDiag(d) {
    const box = $('#pp-diag-inline'); if (!box) return;
    box.innerHTML = ''; box.appendChild(renderDiagCard(d));
  }

  // ── Processes tab
  function renderProc(body) {
    body.appendChild(html`
      <div class="pp-card">
        <h4>Processes <button class="pp-btn small" id="pp-proc-refresh"><i class="fas fa-rotate"></i></button></h4>
        <div id="pp-proc-wrap"><div class="pp-skel"></div><div class="pp-skel"></div><div class="pp-skel"></div></div>
      </div>`);
    $('#pp-proc-refresh').addEventListener('click', loadProc);
    loadProc();
  }
  async function loadProc() {
    const f = detectFolder(); if (!f) return;
    const r = await api(`/server/processes/${f}`);
    const wrap = $('#pp-proc-wrap'); if (!wrap) return;
    if (!r.processes || !r.processes.length) {
      wrap.innerHTML = '<div class="pp-muted">No running processes.</div>'; return;
    }
    const tbl = html`<table class="pp-proc"><thead><tr><th>PID</th><th>Name</th><th>CPU</th><th>RAM</th><th>Thr</th><th></th></tr></thead><tbody></tbody></table>`;
    r.processes.forEach(p => {
      const tr = html`<tr>
        <td class="n">${p.pid}</td><td>${esc(p.name)}</td>
        <td>${p.cpu.toFixed(1)}%</td><td>${p.ram_mb} MB</td><td>${p.threads}</td>
        <td><button class="pp-btn small danger" data-pid="${p.pid}"><i class="fas fa-xmark"></i></button></td>
      </tr>`;
      tr.querySelector('button').addEventListener('click', async (e) => {
        const pid = e.currentTarget.dataset.pid;
        await api(`/server/kill-pid/${f}`, { method: 'POST', json: { pid: +pid } });
        loadProc();
      });
      tbl.querySelector('tbody').appendChild(tr);
    });
    wrap.innerHTML = ''; wrap.appendChild(tbl);
  }

  // ── AI tab
  function renderDiagCard(d) {
    const card = html`
      <div class="pp-diag">
        <span class="conf">${d.confidence||0}% confidence</span>
        <span class="badge err">${esc(d.type||'Error')}</span>
        <div class="t">${esc(d.title||'')}</div>
        <div class="e">${esc(d.explanation||'')}</div>
        <div class="e" style="margin-top:6px"><b>Fix:</b> ${esc(d.fix||'')}</div>
        ${d.command ? `<div class="cmd"><span>${esc(d.command)}</span>
          <span style="display:flex;gap:4px;flex-shrink:0">
            <button class="pp-btn small" data-copy="${esc(d.command)}"><i class="fas fa-copy"></i></button>
            <button class="pp-btn small primary" data-install="${esc(d.command)}"><i class="fas fa-download"></i></button>
          </span></div>` : ''}
      </div>`;
    card.querySelectorAll('[data-copy]').forEach(b => b.addEventListener('click', () => {
      navigator.clipboard?.writeText(b.dataset.copy);
      b.innerHTML = '<i class="fas fa-check"></i>';
      setTimeout(() => b.innerHTML = '<i class="fas fa-copy"></i>', 1200);
    }));
    card.querySelectorAll('[data-install]').forEach(b => b.addEventListener('click', async () => {
      const cmd = b.dataset.install || '';
      const m = cmd.match(/pip install(?: --user)? +([^\s].*)$/);
      if (!m) return;
      const f = detectFolder(); if (!f) return;
      b.disabled = true; b.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
      const res = await api(`/server/install/${f}`, { method: 'POST', json: { package: m[1] } });
      b.disabled = false;
      b.innerHTML = res.status === 'success' ? '<i class="fas fa-check"></i>' : '<i class="fas fa-triangle-exclamation"></i>';
    }));
    return card;
  }
  function renderAI(body) {
    body.appendChild(html`
      <div class="pp-card">
        <h4>AI Auto-Fix <button class="pp-btn small" id="pp-ai-refresh"><i class="fas fa-rotate"></i> Scan</button></h4>
        <div id="pp-ai-wrap"><div class="pp-skel"></div><div class="pp-skel"></div></div>
      </div>`);
    $('#pp-ai-refresh').addEventListener('click', loadAI);
    loadAI();
  }
  async function loadAI() {
    const f = detectFolder(); const wrap = $('#pp-ai-wrap'); if (!f || !wrap) return;
    wrap.innerHTML = '<div class="pp-skel"></div><div class="pp-skel"></div>';
    const r = await api(`/ai/analyze/${f}`);
    wrap.innerHTML = '';
    if (!r.findings || !r.findings.length) {
      wrap.appendChild(html`<div class="pp-diag ok"><span class="badge">Clean</span>
        <div class="t">No known errors detected</div>
        <div class="e">Console output looks healthy. Rerun the scan after your next crash.</div></div>`);
      return;
    }
    r.findings.forEach(d => wrap.appendChild(renderDiagCard(d)));
    if (r.missing_packages && r.missing_packages.length) {
      const cmd = `pip install --user ${r.missing_packages.join(' ')}`;
      wrap.appendChild(html`<div class="pp-diag">
        <span class="badge">Bulk install</span>
        <div class="t">Missing requirements</div>
        <div class="e">Install everything the log flagged in one shot.</div>
        <div class="cmd"><span>${esc(cmd)}</span>
          <button class="pp-btn small primary" id="pp-ai-bulk"><i class="fas fa-download"></i></button>
        </div></div>`);
      $('#pp-ai-bulk').addEventListener('click', async (e) => {
        e.currentTarget.disabled = true;
        for (const p of r.missing_packages) {
          await api(`/server/install/${detectFolder()}`, { method: 'POST', json: { package: p } });
        }
        loadAI();
      });
    }
  }

  // ── More tab (backups, clone, auto-restart, etc.)
  function renderMore(body) {
    body.appendChild(html`
      <div class="pp-card">
        <h4>Auto-restart on crash</h4>
        <div class="pp-row">
          <div><div>Restart automatically when the process exits with an error.</div>
            <div class="pp-muted" id="pp-ar-status">Loading…</div></div>
          <div class="pp-switch" id="pp-ar-toggle"></div>
        </div>
      </div>
      <div class="pp-card">
        <h4>Backups <button class="pp-btn small primary" id="pp-bk-new"><i class="fas fa-floppy-disk"></i> Create</button></h4>
        <ul class="pp-list" id="pp-bk-list"><li class="pp-muted">Loading…</li></ul>
      </div>
      <div class="pp-card">
        <h4>Server</h4>
        <div class="pp-term-toolbar">
          <button class="pp-btn" id="pp-clone"><i class="fas fa-clone"></i> Clone server</button>
          <a class="pp-btn" id="pp-dl-all" href="#"><i class="fas fa-file-zipper"></i> Download all</a>
        </div>
      </div>
      <div class="pp-card">
        <h4>Session</h4>
        <div class="pp-muted">Session auto-locks after 60 minutes of inactivity. Rate limit: 120 req/min.</div>
      </div>`);

    const f = detectFolder();
    // Auto-restart
    (async () => {
      if (!f) return;
      const s = await api(`/server/autorestart/${f}`);
      $('#pp-ar-toggle').classList.toggle('on', !!s.enabled);
      $('#pp-ar-status').textContent = s.enabled ? 'Enabled' : 'Disabled';
    })();
    $('#pp-ar-toggle').addEventListener('click', async (e) => {
      const on = !e.currentTarget.classList.contains('on');
      await api(`/server/autorestart/${f}`, { method: 'POST', json: { enabled: on } });
      e.currentTarget.classList.toggle('on', on);
      $('#pp-ar-status').textContent = on ? 'Enabled' : 'Disabled';
    });

    // Backups
    async function loadBk() {
      const list = $('#pp-bk-list');
      const r = await api(`/server/backups/${f}`);
      list.innerHTML = '';
      if (!r.backups || !r.backups.length) {
        list.appendChild(html`<li class="pp-muted">No backups yet.</li>`); return;
      }
      r.backups.forEach(b => {
        const li = html`<li>
          <div><div>${esc(b.name)}</div><div class="meta">${b.size_human} · ${b.modified}</div></div>
          <div class="actions">
            <a class="pp-btn small" href="/server/backup-download/${f}/${encodeURIComponent(b.name)}"><i class="fas fa-download"></i></a>
            <button class="pp-btn small" data-r="${esc(b.name)}"><i class="fas fa-rotate-left"></i></button>
            <button class="pp-btn small danger" data-d="${esc(b.name)}"><i class="fas fa-trash"></i></button>
          </div></li>`;
        li.querySelector('[data-r]').addEventListener('click', async () => {
          if (!confirm('Restore this backup? Existing files with the same name will be overwritten.')) return;
          await api(`/server/backup-restore/${f}`, { method: 'POST', json: { name: b.name } });
          alert('Restored.');
        });
        li.querySelector('[data-d]').addEventListener('click', async () => {
          if (!confirm('Delete backup?')) return;
          await api(`/server/backup-delete/${f}`, { method: 'POST', json: { name: b.name } });
          loadBk();
        });
        list.appendChild(li);
      });
    }
    $('#pp-bk-new').addEventListener('click', async (e) => {
      e.currentTarget.disabled = true;
      await api(`/server/backup/${f}`, { method: 'POST' });
      e.currentTarget.disabled = false; loadBk();
    });
    if (f) loadBk();

    $('#pp-clone').addEventListener('click', async () => {
      if (!confirm('Create a clone of this server?')) return;
      const r = await api(`/server/clone/${f}`, { method: 'POST' });
      alert(r.status === 'ok' ? `Cloned as ${r.name}` : (r.msg || 'Failed'));
    });
    $('#pp-dl-all').href = `/files/download-folder/${f}?path=&name=`;
  }

  // ─── Boot ──────────────────────────────────────────────
  function boot() {
    if (document.getElementById('pp-fab')) return;
    document.body.setAttribute('data-pp-amoled', '1');
    buildSheet();
    buildFab();
    // Keyboard shortcut: press "P" to open panel
    document.addEventListener('keydown', (e) => {
      if (e.key === 'P' && !/input|textarea/i.test((e.target||{}).tagName || '')) openPanel();
      if (e.key === 'Escape') closePanel();
    });
    // Expose for existing code
    window.ProPanel = { open: openPanel, close: closePanel, api, onTransport,
                        setActiveFolder,
                        get transport() { return transport; } };

    // Open the socket eagerly so servers:status + panel:stats/log events
    // reach the dashboard even before the sheet is opened. If the socket
    // fails to connect, transport stays 'polling' and consumers can fall back.
    ensureSocket(() => {});
    // If a curSrv-style folder can be detected, subscribe now so the
    // dashboard's console/monitor stays live without opening the sheet.
    setTimeout(() => {
      ensureSocket((s) => {
        const f = detectFolder();
        if (s && f) { try { s.emit('term:subscribe', { folder: f }); s.emit('stats:subscribe', { folder: f }); } catch (_) {} }
      });
    }, 300);
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
