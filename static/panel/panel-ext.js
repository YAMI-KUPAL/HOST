/* =========================================================
   CLAUD Pro Panel — EXTENSION LAYER (additive)
   Loaded AFTER panel.js. Uses window.ProPanel + panel:* events.
   - Live "bot polling" status widget in the dashboard
   - Diagnose button (AI quickfix, inline)
   - Socket reconnect toast + exponential backoff
   - Uptime sparkline (24h history)
   - Console: severity filter proxy + "download last N minutes"
   - Theme toggle (dark <-> light)
   - No new polling loops — all live data via panel:stats / panel:log
   ========================================================= */
(function () {
  'use strict';
  if (window.__panelExtLoaded) return;
  window.__panelExtLoaded = true;

  const $  = (s, r=document) => r.querySelector(s);
  const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));
  const esc = (s) => String(s==null?'':s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const el = (h) => { const t=document.createElement('template'); t.innerHTML=h.trim(); return t.content.firstElementChild; };

  function detectFolder() {
    if (window.__panelActiveFolder) return window.__panelActiveFolder;
    if (window.curSrv && window.curSrv.folder) return window.curSrv.folder;
    const p = new URLSearchParams(location.search);
    return p.get('folder') || '';
  }
  function csrf() {
    const i = document.querySelector('input[name="csrf_token"]');
    return i ? i.value : (window.CSRF_TOKEN || '');
  }
  async function api(path, opts={}) {
    opts.headers = Object.assign({'X-CSRF-Token': csrf()}, opts.headers||{});
    if (opts.json !== undefined) {
      opts.body = JSON.stringify(opts.json);
      opts.headers['Content-Type'] = 'application/json';
      delete opts.json;
    }
    const r = await fetch(path, opts);
    try { return await r.json(); } catch { return {}; }
  }

  // ── Styles ────────────────────────────────────────────────────
  const CSS = `
  .bot-status-card{
    background:linear-gradient(180deg,#0d1220,#0a0f1a);
    border:1px solid #1e2740;border-radius:14px;padding:12px 14px;
    margin:0 0 14px 0;color:#e6edf7;
    box-shadow:0 6px 18px rgba(0,0,0,.28);
    font-family:'Inter',system-ui,sans-serif;
  }
  .bot-status-head{display:flex;align-items:center;gap:10px;font-size:12px;font-weight:700;letter-spacing:.3px}
  .bot-status-dot{width:9px;height:9px;border-radius:50%;background:#6b7280;box-shadow:0 0 0 0 rgba(0,0,0,0);transition:background .3s}
  .bot-status-dot.pulse{animation:bs-pulse 1.2s ease-out infinite}
  .bot-status-dot.on{background:#22e07a;box-shadow:0 0 10px rgba(34,224,122,.65)}
  .bot-status-dot.warn{background:#ffb020;box-shadow:0 0 10px rgba(255,176,32,.6)}
  .bot-status-dot.off{background:#ff4d5e;box-shadow:0 0 10px rgba(255,77,94,.55)}
  @keyframes bs-pulse{0%{box-shadow:0 0 0 0 rgba(126,200,255,.55)}70%{box-shadow:0 0 0 10px rgba(126,200,255,0)}100%{box-shadow:0 0 0 0 rgba(126,200,255,0)}}
  .bot-status-text{flex:1;font-size:12px}
  .bot-status-meta{font-size:10.5px;color:#7f8aa3;font-weight:500}
  .bot-status-uptime{font-family:'JetBrains Mono',monospace;font-size:10.5px;color:#7ec8ff}
  .bot-mini-log{
    margin-top:8px;background:#05080f;border:1px solid #131b30;border-radius:8px;
    padding:6px 8px;font-family:'JetBrains Mono','Fira Code',monospace;font-size:10.5px;
    color:#c7d2e0;max-height:52px;overflow:hidden;line-height:1.45;
    display:flex;flex-direction:column;justify-content:flex-end;
  }
  .bot-mini-log.pulse{animation:bs-glow 2.2s ease-in-out infinite}
  @keyframes bs-glow{0%,100%{box-shadow:inset 0 0 0 1px rgba(126,200,255,.08)}50%{box-shadow:inset 0 0 0 1px rgba(126,200,255,.28)}}
  .bot-mini-log .l{white-space:pre-wrap;word-break:break-word;opacity:.88}
  .bot-mini-log .l.err{color:#ff8ba0}
  .bot-mini-log .l.warn{color:#ffd166}
  .bot-status-spark{margin-top:8px}
  .bot-status-spark svg{width:100%;height:32px;display:block}

  .diag-inline-box{
    margin:8px 0 0;background:#0a1020;border:1px solid #1f2a44;border-radius:10px;
    padding:10px 12px;color:#e6edf7;font-size:12px
  }
  .diag-inline-box .t{font-weight:700;margin-bottom:4px;color:#7ec8ff}
  .diag-inline-box .cmd{
    margin-top:6px;background:#05080f;padding:6px 8px;border-radius:6px;
    font-family:'JetBrains Mono',monospace;font-size:11px;color:#7ff0b0;
    display:flex;justify-content:space-between;gap:6px;align-items:center
  }
  .diag-inline-box button{
    background:#152040;color:#dbe4ff;border:1px solid #253560;padding:4px 8px;
    border-radius:6px;font-size:11px;cursor:pointer
  }

  .pp-toast{
    position:fixed;left:50%;bottom:calc(env(safe-area-inset-bottom,0) + 76px);
    transform:translateX(-50%);background:#101827;color:#e6edf7;
    border:1px solid #253560;border-radius:999px;padding:8px 14px;
    font-size:12px;font-family:'Inter',system-ui,sans-serif;
    box-shadow:0 8px 24px rgba(0,0,0,.4);z-index:9999;
    display:flex;align-items:center;gap:8px;opacity:0;pointer-events:none;
    transition:opacity .25s ease, transform .25s ease
  }
  .pp-toast.show{opacity:1;transform:translateX(-50%) translateY(0)}
  .pp-toast .d{width:8px;height:8px;border-radius:50%;background:#ffb020;animation:bs-pulse 1.2s infinite}

  .pp-theme-btn{position:fixed;top:12px;right:12px;z-index:600;
    background:#101827;color:#e6edf7;border:1px solid #253560;
    width:34px;height:34px;border-radius:10px;font-size:14px;cursor:pointer}

  body.theme-light{background:#f6f7fb;color:#0d1220}
  body.theme-light .bot-status-card{background:#fff;border-color:#e2e6ef;color:#0d1220;box-shadow:0 4px 14px rgba(10,20,40,.06)}
  body.theme-light .bot-mini-log{background:#f0f2f8;border-color:#e2e6ef;color:#1a2033}
  `;
  document.head.appendChild(el(`<style>${CSS}</style>`));

  // ── Toast helper ─────────────────────────────────────────────
  let toastEl = null, toastTimer = null;
  function toast(msg, {sticky=false, kind='warn'}={}){
    if (!toastEl) {
      toastEl = el(`<div class="pp-toast"><span class="d"></span><span class="m"></span></div>`);
      document.body.appendChild(toastEl);
    }
    toastEl.querySelector('.m').textContent = msg;
    toastEl.classList.add('show');
    clearTimeout(toastTimer);
    if (!sticky) toastTimer = setTimeout(()=>toastEl.classList.remove('show'), 2600);
  }
  function toastHide(){ if(toastEl) toastEl.classList.remove('show'); clearTimeout(toastTimer); }

  // ── Socket reconnect resilience ──────────────────────────────
  // panel.js exposes ProPanel.onTransport. We layer exp backoff manual retry
  // on top for the case where io()'s built-in reconnect gives up.
  function wireReconnect(){
    if (!window.ProPanel) return;
    let attempts = 0, showTimer = null;
    ProPanel.onTransport((t) => {
      if (t === 'socket') {
        attempts = 0;
        clearTimeout(showTimer);
        toastHide();
      } else {
        // Only show toast if we stay in polling for >2s (avoid flicker on load)
        clearTimeout(showTimer);
        showTimer = setTimeout(()=>toast('Reconnecting…', {sticky:true}), 2000);
        attempts++;
      }
    });
  }

  // ── Bot status widget ────────────────────────────────────────
  const MAX_LOG_LINES = 3;
  const state = {
    hadFirstStats: false, hadFirstLog: false, online: false,
    startedAt: Date.now(), logLines: [], hist: [],
  };

  function ensureWidget(){
    // Manage view is the per-server area. Mount just above the console.
    const consoleWrap = document.getElementById('consoleWrap');
    if (!consoleWrap || document.getElementById('botStatusCard')) return null;
    const card = el(`
      <div class="bot-status-card" id="botStatusCard">
        <div class="bot-status-head">
          <span class="bot-status-dot pulse" id="bsDot"></span>
          <span class="bot-status-text" id="bsText">Starting…</span>
          <span class="bot-status-uptime" id="bsUp"></span>
        </div>
        <div class="bot-status-meta" id="bsMeta">Waiting for the process to report in.</div>
        <div class="bot-mini-log pulse" id="bsLog"></div>
        <div class="bot-status-spark"><svg id="bsSpark" viewBox="0 0 200 32" preserveAspectRatio="none">
          <polyline id="bsSparkLine" fill="none" stroke="#7ec8ff" stroke-width="1.5" points="" />
        </svg></div>
        <div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap">
          <button id="bsDiag" class="con-btn" style="padding:5px 10px;font-size:11px">
            <i class="fas fa-wand-magic-sparkles"></i> Diagnose
          </button>
          <button id="bsExportRange" class="con-btn" style="padding:5px 10px;font-size:11px">
            <i class="fas fa-download"></i> Last <span id="bsRange">5</span> min
          </button>
          <input id="bsRangeInput" type="number" min="1" max="360" value="5"
                 style="width:56px;background:#05080f;color:#e6edf7;border:1px solid #253560;border-radius:6px;padding:3px 6px;font-size:11px"/>
        </div>
        <div id="bsDiagOut"></div>
      </div>
    `);
    consoleWrap.parentNode.insertBefore(card, consoleWrap);
    card.querySelector('#bsDiag').addEventListener('click', runDiagnose);
    card.querySelector('#bsExportRange').addEventListener('click', exportRange);
    card.querySelector('#bsRangeInput').addEventListener('input', (e)=>{
      const v = Math.max(1, Math.min(360, parseInt(e.target.value||'5',10)||5));
      card.querySelector('#bsRange').textContent = v;
    });
    loadHistory();
    return card;
  }

  function setStatus(kind, text, meta){
    const dot = $('#bsDot'), t = $('#bsText'), m = $('#bsMeta'), log = $('#bsLog');
    if (!dot) return;
    dot.classList.remove('on','off','warn');
    dot.classList.add(kind);
    if (kind === 'on') { dot.classList.remove('pulse'); log && log.classList.remove('pulse'); }
    else               { dot.classList.add('pulse');    log && log.classList.add('pulse'); }
    t.textContent = text;
    if (meta != null) m.textContent = meta;
  }

  function fmtUptime(s){
    if (!s || s < 0) return '';
    const h=Math.floor(s/3600), m=Math.floor(s%3600/60), sec=s%60;
    return (h?`${h}h `:'') + (m?`${m}m `:'') + `${sec}s`;
  }

  // Called via panel:stats event bus (existing) — NOT a new poll loop.
  function onStats(p){
    if (!ensureWidget()) return;
    state.hadFirstStats = true;
    state.online = !!p.online;
    if (p.online) {
      setStatus('on', 'Your bot is running ✅',
        `PID ${p.pid||'—'} · CPU ${(p.cpu||0).toFixed(1)}% · RAM ${(p.ram_mb||0).toFixed(0)} MB`);
      $('#bsUp').textContent = fmtUptime(p.uptime_s);
    } else if (p.crashed) {
      setStatus('off', 'Crashed — waiting to restart', 'Auto-restart will retry shortly.');
      $('#bsUp').textContent = '';
    } else {
      setStatus('warn', state.hadFirstLog ? 'Polling for updates…' : 'Starting…',
        'Process not detected yet.');
      $('#bsUp').textContent = '';
    }
    // sparkline sample (CPU %)
    pushHist(p.cpu || 0);
    drawSpark();
  }

  function onLog(payload){
    if (!ensureWidget()) return;
    state.hadFirstLog = true;
    const chunk = (payload && payload.chunk) || '';
    if (!chunk) return;
    const rows = chunk.split(/\r?\n/).filter(Boolean);
    for (const r of rows) {
      const kind = /error|traceback|exception/i.test(r) ? 'err'
                : /warn/i.test(r) ? 'warn' : '';
      state.logLines.push({t: r.slice(0,220), k: kind});
    }
    while (state.logLines.length > MAX_LOG_LINES) state.logLines.shift();
    const box = $('#bsLog'); if (!box) return;
    // rAF batch
    if (!onLog._raf) {
      onLog._raf = requestAnimationFrame(()=>{
        onLog._raf = 0;
        box.innerHTML = state.logLines
          .map(l => `<div class="l ${l.k}">${esc(l.t)}</div>`).join('');
      });
    }
  }

  // ── Uptime history / sparkline ───────────────────────────────
  async function loadHistory(){
    const f = detectFolder(); if (!f) return;
    try {
      const r = await api(`/server/stats-history/${f}`);
      if (r && r.samples) {
        state.hist = r.samples.slice(-120);
        drawSpark();
      }
    } catch(_){}
  }
  function pushHist(v){
    state.hist.push(v);
    if (state.hist.length > 120) state.hist.shift();
  }
  function drawSpark(){
    const line = $('#bsSparkLine'); if (!line || !state.hist.length) return;
    const w=200, h=32, n=state.hist.length;
    const max = Math.max(10, ...state.hist);
    const pts = state.hist.map((v,i)=>{
      const x = (i/(Math.max(1,n-1)))*w;
      const y = h - (v/max)*(h-2) - 1;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(' ');
    line.setAttribute('points', pts);
  }

  // ── Diagnose (AI) ────────────────────────────────────────────
  async function runDiagnose(){
    const f = detectFolder(); if (!f) return;
    const out = $('#bsDiagOut');
    out.innerHTML = `<div class="diag-inline-box"><div class="t">Analyzing recent log…</div></div>`;
    try {
      const r = await api(`/ai/quickfix/${f}`, {method:'POST'});
      if (r.status === 'ok' && r.diagnosis) {
        const d = r.diagnosis;
        out.innerHTML = '';
        const box = el(`
          <div class="diag-inline-box">
            <div class="t">${esc(d.title || 'Diagnosis')}</div>
            <div>${esc(d.explanation || '')}</div>
            <div style="margin-top:4px"><b>Fix:</b> ${esc(d.fix || '')}</div>
            ${d.command ? `<div class="cmd"><span>${esc(d.command)}</span>
              <button data-copy="${esc(d.command)}"><i class="fas fa-copy"></i></button></div>`:''}
          </div>
        `);
        out.appendChild(box);
        box.querySelectorAll('[data-copy]').forEach(b=>b.addEventListener('click',()=>{
          navigator.clipboard?.writeText(b.dataset.copy);
          b.innerHTML='<i class="fas fa-check"></i>';
        }));
      } else {
        out.innerHTML = `<div class="diag-inline-box"><div class="t">No known error detected</div>
          <div>Recent log looks clean. Try again after the next crash.</div></div>`;
      }
    } catch (e) {
      out.innerHTML = `<div class="diag-inline-box"><div class="t">Diagnose failed</div>
        <div>${esc(e.message||e)}</div></div>`;
    }
  }

  // ── Export last N minutes ────────────────────────────────────
  async function exportRange(){
    const f = detectFolder(); if (!f) return;
    const mins = parseInt($('#bsRangeInput').value||'5',10) || 5;
    try {
      const r = await fetch(`/server/log-range/${f}?minutes=${mins}`, {headers:{'X-CSRF-Token':csrf()}});
      const blob = await r.blob();
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = `${f}-last${mins}min.log`;
      a.click();
    } catch(e){ toast('Export failed'); }
  }

  // ── Theme toggle ─────────────────────────────────────────────
  function buildThemeToggle(){
    if (document.getElementById('themeBtn')) return;
    const btn = el(`<button class="pp-theme-btn" id="themeBtn" title="Toggle theme"><i class="fas fa-moon"></i></button>`);
    document.body.appendChild(btn);
    const saved = localStorage.getItem('panelTheme') || 'dark';
    if (saved === 'light') document.body.classList.add('theme-light');
    btn.querySelector('i').className = 'fas ' + (saved==='light'?'fa-sun':'fa-moon');
    btn.addEventListener('click', ()=>{
      document.body.classList.toggle('theme-light');
      const now = document.body.classList.contains('theme-light') ? 'light' : 'dark';
      localStorage.setItem('panelTheme', now);
      btn.querySelector('i').className = 'fas ' + (now==='light'?'fa-sun':'fa-moon');
    });
  }

  // ── Boot ─────────────────────────────────────────────────────
  function boot(){
    buildThemeToggle();
    wireReconnect();
    window.addEventListener('panel:stats', (e)=> onStats(e.detail || {}));
    window.addEventListener('panel:log',   (e)=> onLog(e.detail || {}));
    // Try to mount widget now (in case manage view already visible) and
    // re-try when the user opens a server. openSrv() sets curSrv globally.
    const tryMount = () => ensureWidget();
    tryMount();
    // Observe the manage view flipping to display:block once user picks a server.
    const mv = document.getElementById('manageView');
    if (mv) new MutationObserver(tryMount).observe(mv, {attributes:true, attributeFilter:['style']});
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
