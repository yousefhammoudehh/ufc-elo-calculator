/* global document, window, fetch, alert */

async function fetchJSON(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data && data.message) || `Request failed ${res.status}`);
  return data.data ?? data;
}

function q(sel){ return document.querySelector(sel); }
function fmt2(x){ if (x === null || x === undefined) return '—'; return Number(x).toFixed(2); }

async function resolveFighterId(input) {
  if (!input) throw new Error('Missing fighter');
  // If looks like an ID, use directly; else search
  if (!/\s/.test(input) && input.length >= 8) return input;
  try {
    const res = await fetchJSON(`/fighters/search?q=${encodeURIComponent(input)}&limit=1`);
    if (Array.isArray(res) && res.length) return res[0].fighter_id;
  } catch {}
  throw new Error('Fighter not found');
}

function renderFormBlock(fi) {
  const el = q('#fiBlock');
  if (!el) return;
  el.innerHTML = `
    <div>FI: <strong>${fmt2(fi.fi)}</strong> (over last ${fi.count} fights)</div>
    ${fi.avg_opp_elo ? `<div>Avg opponent ELO: ${fmt2(fi.avg_opp_elo)}</div>` : ''}
  `;
  // Sparkline of residuals
  try {
    const canvas = q('#fiSpark');
    if (canvas && Array.isArray(fi.series)) {
      const ctx = canvas.getContext('2d');
      const data = fi.series.map(x => x.residual);
      if (window.__fiSpark) window.__fiSpark.destroy();
      window.__fiSpark = new Chart(ctx, {
        type: 'line',
        data: { labels: fi.series.map(x => x.date || ''), datasets: [{ data, borderColor: '#3ba3ff', tension: 0.3, pointRadius: 0, fill: false }] },
        options: { plugins: { legend: { display: false }, tooltip: { callbacks: { label: (ctx) => `Residual: ${Number(ctx.parsed.y).toFixed(3)}` } } }, scales: { x: { display: false }, y: { display: false } }, elements: { line: { borderWidth: 2 } } },
      });
    }
  } catch {}
}

function renderMomentumBlock(m) {
  const el = q('#momBlock'); if (!el) return;
  el.innerHTML = `
    <div>Slope per fight: <strong>${fmt2(m.slope_per_fight)}</strong></div>
    <div>Slope per 180 days: <strong>${m.slope_per_180d != null ? fmt2(m.slope_per_180d) : '—'}</strong></div>
  `;
  // Sparkline of recent elo
  try {
    const canvas = q('#momSpark');
    if (canvas && Array.isArray(m.series)) {
      const ctx = canvas.getContext('2d');
      const data = m.series.map(x => x.elo);
      if (window.__momSpark) window.__momSpark.destroy();
      window.__momSpark = new Chart(ctx, {
        type: 'line',
        data: { labels: m.series.map(x => x.date || ''), datasets: [{ data, borderColor: '#10b981', tension: 0.3, pointRadius: 0, fill: false }] },
        options: { plugins: { legend: { display: false }, tooltip: { callbacks: { label: (ctx) => `ELO: ${Number(ctx.parsed.y).toFixed(2)}` } } }, scales: { x: { display: false }, y: { display: false } }, elements: { line: { borderWidth: 2 } } },
      });
    }
  } catch {}
}

function renderRatesTbl(r) {
  const el = q('#ratesTbl'); if (!el) return;
  const per15 = (q('#rateMode')?.value || 'per_min') === 'per_15';
  const rows = [
    ['Minutes', fmt2(r.minutes)],
    ['Sig. landed/' + (per15?'15':'min'), fmt2((r.rates?.sig_landed_per_min||0) * (per15?15:1))],
    ['Sig. thrown/' + (per15?'15':'min'), fmt2((r.rates?.sig_thrown_per_min||0) * (per15?15:1))],
    ['KD/' + (per15?'15':'min'), fmt2((r.rates?.kd_per_min||0) * (per15?15:1))],
    ['TD/' + (per15?'15':'min'), fmt2((r.rates?.td_per_min||0) * (per15?15:1))],
    ['TD att/' + (per15?'15':'min'), fmt2((r.rates?.td_att_per_min||0) * (per15?15:1))],
    ['Sub att/' + (per15?'15':'min'), fmt2((r.rates?.sub_att_per_min||0) * (per15?15:1))],
    ['TD%', r.rates?.td_pct != null ? (Number(r.rates.td_pct) * 100).toFixed(1) + '%' : '—'],
    ['Control share', r.rates?.control_share != null ? (Number(r.rates.control_share) * 100).toFixed(1) + '%' : '—'],
  ];
  el.innerHTML = `<thead><tr><th>Metric</th><th>Value</th></tr></thead>` +
    `<tbody>${rows.map(r => `<tr><td>${r[0]}</td><td>${r[1]}</td></tr>`).join('')}</tbody>`;
}

// Plus/Minus and Consistency blocks
async function loadPlusMinus(fid) {
  const pm = await fetchJSON(`/analytics/plusminus?fighter_id=${encodeURIComponent(fid)}&metric=sig_strikes&opp_window_months=18`);
  const el = q('#pmBlock'); if (el) el.innerHTML = `<div>Sig. Str per‑min ±: <strong>${fmt2(pm.plus_minus_per_min)}</strong> (N=${pm.samples})</div>`;
}
async function loadConsistency(fid) {
  const cv = await fetchJSON(`/analytics/consistency-versatility?fighter_id=${encodeURIComponent(fid)}&k=6`);
  const el = q('#cvBlock'); if (!el) return;
  el.innerHTML = `
    <div>SD(ΔELO): <strong>${fmt2(cv.sd_elo_delta)}</strong></div>
    <div>CV (sig/min): <strong>${fmt2(cv.cv_sig_per_min)}</strong>; TD/min: <strong>${fmt2(cv.cv_td_per_min)}</strong>; Ctrl/min: <strong>${fmt2(cv.cv_ctrl_per_min)}</strong></div>
    <div>Versatility (entropy): <strong>${fmt2(cv.versatility)}</strong></div>
  `;
}

async function loadAll(fid){
  // Default to whole-career windows unless user later changes controls
  const fiWin = (q('#fiWindow')?.value || 'fights');
  const fiNraw = Number(q('#fiN')?.value || 7300);
  const fiHLraw = Number(q('#fiHL')?.value || 2000);
  // Clamp to API limits: n <= 7300, half_life_days <= 2000
  const fiN = Math.min(7300, Math.max(1, fiNraw));
  const fiHL = Math.min(2000, Math.max(1, fiHLraw));
  const [fi, mom, rates, sos, style] = await Promise.all([
    fetchJSON(`/analytics/form?fighter_id=${encodeURIComponent(fid)}&window=${encodeURIComponent(fiWin)}&n=${encodeURIComponent(fiN)}&half_life_days=${encodeURIComponent(fiHL)}`),
    // Momentum endpoint caps k <= 50; use max window allowed
    fetchJSON(`/analytics/momentum?fighter_id=${encodeURIComponent(fid)}&k=50`),
    fetchJSON(`/analytics/rates?fighter_id=${encodeURIComponent(fid)}`),
    fetchJSON(`/analytics/sos?fighter_id=${encodeURIComponent(fid)}&window=${encodeURIComponent('fights')}&n=${encodeURIComponent(9999)}`),
    fetchJSON(`/analytics/style-profile?fighter_id=${encodeURIComponent(fid)}`),
  ]);
  renderFormBlock(fi);
  renderMomentumBlock(mom);
  renderRatesTbl(rates);
  renderSos(sos);
  renderStyle(style);
  await Promise.all([loadPlusMinus(fid), loadConsistency(fid), loadHazard(fid)]);
}

async function main(){
  const btn = q('#loadBtn');
  // Attach floating autocomplete + Enter handler
  try { setupAutocomplete('fighterInput'); } catch {}
  const inp = q('#fighterInput');
  inp?.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); btn?.click(); }});
  btn?.addEventListener('click', async () => {
    try {
      const raw = q('#fighterInput')?.value?.trim();
      const fid = await resolveFighterId(raw);
      await loadAll(fid);
      // Wire Quality Wins apply
      const qwb = q('#qwBtn');
      qwb?.addEventListener('click', async () => {
        try {
          const thr = Number(q('#qwThresh')?.value || 1600);
          const rw = await fetchJSON(`/analytics/quality-wins?fighter_id=${encodeURIComponent(fid)}&elo_threshold=${encodeURIComponent(thr)}`);
          renderQualityWins(rw);
        } catch (e) { alert(String(e?.message || e)); }
      });
    } catch (e) {
      alert(String(e?.message || e));
    }
  });
}

main();

// Floating autocomplete adapted from H2H
function setupAutocomplete(inputId) {
  const inp = document.getElementById(inputId);
  if (!inp) return;
  const list = document.createElement('div');
  list.className = 'ac-list';
  document.body.appendChild(list);
  let visible = false;
  let anchorRect = null;
  let items = [];

  function position() {
    const r = inp.getBoundingClientRect();
    anchorRect = r;
    list.style.left = `${r.left}px`;
    list.style.top = `${r.bottom}px`;
    list.style.width = `${r.width}px`;
  }

  function hide() { list.style.display = 'none'; visible = false; items = []; list.innerHTML = ''; }
  function show() { position(); list.style.display = 'block'; visible = true; }

  async function query() {
    const qv = inp.value.trim();
    if (!qv) { hide(); return; }
    try {
      const res = await fetchJSON(`/fighters/search?q=${encodeURIComponent(qv)}&limit=6`);
      items = Array.isArray(res) ? res : [];
      if (!items.length) { hide(); return; }
      list.innerHTML = items.map(it => `<div class="ac-item" data-id="${it.fighter_id}">${it.name || it.fighter_id}</div>`).join('');
      show();
    } catch { hide(); }
  }

  inp.addEventListener('input', () => { query().catch(()=>{}); });
  window.addEventListener('resize', () => { if (visible) position(); });
  document.addEventListener('click', (e) => {
    if (e.target === inp) return;
    if (!list.contains(e.target)) hide();
  });
  list.addEventListener('click', (e) => {
    const el = e.target.closest('.ac-item');
    if (!el) return;
    const id = el.getAttribute('data-id');
    if (id) {
      inp.value = items.find(x => x.fighter_id === id)?.name || id;
      hide();
    }
  });
}

function renderSos(s) {
  const el = q('#sosBlock'); if (!el) return;
  el.innerHTML = `
    <div>Mean opp ELO: <strong>${fmt2(s.mean)}</strong></div>
    <div>Median: <strong>${fmt2(s.median)}</strong> (p25 ${fmt2(s.p25)} • p75 ${fmt2(s.p75)})</div>
    <div>Sample size: ${s.count}</div>
  `;
}

function renderStyle(st) {
  const el = q('#styleBlock'); if (!el) return;
  const mix = st.phase_mix || {};
  el.innerHTML = `
    <div>Control share: <strong>${st.control_share != null ? (st.control_share*100).toFixed(1)+'%' : '—'}</strong></div>
    <div>Phase mix: distance ${mix.distance_pct != null ? (mix.distance_pct*100).toFixed(1)+'%' : '—'}, clinch ${mix.clinch_pct != null ? (mix.clinch_pct*100).toFixed(1)+'%' : '—'}, ground ${mix.ground_pct != null ? (mix.ground_pct*100).toFixed(1)+'%' : '—'}</div>
  `;
}

function renderQualityWins(rw){
  const el = q('#qwBlock'); if (!el) return;
  const pct = rw.share != null ? (rw.share * 100).toFixed(1) + '%' : '—';
  el.innerHTML = `
    <div>Wins vs ELO ≥ ${fmt2(rw.threshold)}: <strong>${rw.wins_q}</strong> / ${rw.wins_total} (${pct})</div>
  `;
}

async function loadHazard(fid){
  try {
    const hz = await fetchJSON(`/analytics/hazard?fighter_id=${encodeURIComponent(fid)}&five_round=auto`);
    const cvs = q('#hazardChart');
    const meta = q('#hazardMeta');
    if (!cvs) return;
    const ctx = cvs.getContext('2d');
    const labels = (hz.bins||[]).map(b => `${b.lo}-${b.hi}m`);
    const ko = (hz.bins||[]).map(b => b.ko||0);
    const sub = (hz.bins||[]).map(b => b.sub||0);
    const against = (hz.bins||[]).map(b => b.finished_against||0);
    const total = [...ko, ...sub, ...against].reduce((a,b)=>a+Number(b||0),0);
    if (!total) {
      if (meta) meta.textContent = 'No finish events recorded across career — nothing to plot.';
      if (window.__hazardChart) { window.__hazardChart.destroy(); }
      return;
    }
    if (window.__hazardChart) window.__hazardChart.destroy();
    window.__hazardChart = new Chart(ctx, {
      type: 'bar',
      data: { labels, datasets: [
        { label: 'KO/TKO (for)', data: ko, backgroundColor: 'rgba(59,163,255,0.6)' },
        { label: 'Sub (for)', data: sub, backgroundColor: 'rgba(16,185,129,0.6)' },
        { label: 'Finished (against)', data: against, backgroundColor: 'rgba(239,68,68,0.5)' },
      ]},
      options: { plugins:{ legend:{ display:true } }, scales:{ x:{ stacked:false }, y:{ beginAtZero:true, suggestedMax: Math.max(1, Math.max(...ko, ...sub, ...against)) } } },
    });
    if (meta) meta.textContent = `Durability (1 − finishes per 15m): ${fmt2(hz.durability)}`;
  } catch (e) {
    const meta = q('#hazardMeta'); if (meta) meta.textContent = 'Hazard data unavailable.';
  }
}
