async function fetchJSON(url) {
  const doFetch = async (u) => fetch(u);
  let res = await doFetch(url);
  // Fallback: try /api prefix if 404 and not already prefixed (supports reverse proxies)
  if (res.status === 404 && typeof url === 'string' && url.startsWith('/') && !url.startsWith('/api/')) {
    res = await doFetch(`/api${url}`);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  // API envelope: { status_code, message, data }
  if (!payload) throw new Error('Empty response');
  if (typeof payload.status_code === 'number' && payload.status_code !== 200) {
    throw new Error(payload.message || `HTTP ${payload.status_code}`);
  }
  return payload.data !== undefined ? payload.data : payload;
}

// ---- Loading helpers ----
function findCard(el) {
  let cur = el;
  while (cur && cur !== document.body) {
    if (cur.classList && cur.classList.contains('card')) return cur;
    cur = cur.parentElement;
  }
  return null;
}

function showLoaderFor(el, text = 'Loading…') {
  const card = findCard(el) || el;
  if (!card) return null;
  // Avoid stacking
  const existing = card.querySelector(':scope > .loader-overlay');
  if (existing) return existing;
  const overlay = document.createElement('div');
  overlay.className = 'loader-overlay';
  overlay.innerHTML = `<div class="loader"><span class="spinner"></span><span>${text}</span></div>`;
  card.appendChild(overlay);
  return overlay;
}

function hideLoaderFor(el) {
  const card = findCard(el) || el;
  if (!card) return;
  const overlay = card.querySelector(':scope > .loader-overlay');
  if (overlay) overlay.remove();
}

function dateMinusDays(isoDateStr, days) {
  if (!isoDateStr) return isoDateStr;
  const d = new Date(isoDateStr);
  if (Number.isNaN(d.getTime())) return isoDateStr;
  d.setUTCDate(d.getUTCDate() - days);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

const DIVISION_LABELS = {
  101: "Men's Flyweight (125)",
  102: "Men's Bantamweight (135)",
  103: "Men's Featherweight (145)",
  104: "Men's Lightweight (155)",
  105: "Men's Welterweight (170)",
  106: "Men's Middleweight (185)",
  107: "Men's Light Heavyweight (205)",
  108: "Men's Heavyweight (265)",
  201: "Women's Strawweight (115)",
  202: "Women's Flyweight (125)",
  203: "Women's Bantamweight (135)",
  204: "Women's Featherweight (145)",
};

const MEN_DIVISION_CODES = [101, 102, 103, 104, 105, 106, 107, 108];

function formatDivision(code) {
  if (code == null || code === '') return '—';
  const key = Number(code);
  return DIVISION_LABELS[key] || String(code);
}

function renderTopEloChart(items, label = 'Value', color = {bg: 'rgba(59, 163, 255, 0.6)', stroke: 'rgba(59, 163, 255, 1)'}) {
  const el = document.getElementById('topEloChart');
  if (!el) return;
  const ctx = el.getContext('2d');
  const labels = items.map((x) => x.name);
  const values = items.map((x) => Number(x.value ?? 0));
  const ids = items.map((x) => x.fighter_id);
  if (window.__topEloChart) window.__topEloChart.destroy();
  window.__topEloChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label,
          data: values,
          ids,
          backgroundColor: color.bg,
          borderColor: color.stroke,
          borderWidth: 1,
        },
      ],
    },
    options: {
      onClick: (evt, elements, chart) => {
        const el = elements && elements[0];
        if (!el) return;
        const id = chart.data.datasets[el.datasetIndex].ids[el.index];
        if (id) window.location = `/viz/fighter.html?id=${encodeURIComponent(id)}`;
      },
      scales: {
        y: { beginAtZero: false },
      },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}`,
          },
        },
      },
    },
  });
}

function renderFighterEloChart(hist) {
  const ctx = document.getElementById('fighterEloChart').getContext('2d');
  let labels = hist.points.map((p) => p.event_date || '');
  let values = hist.points.map((p) => p.elo_after ?? p.elo_before ?? null);
  const entry = typeof hist.entry_elo === 'number' ? hist.entry_elo : null;

  // Insert a starting ELO point one week before the first fight
  if (entry != null && labels.length > 0) {
    const preDate = dateMinusDays(labels[0], 7);
    labels = [preDate, ...labels];
    values = [entry, ...values];
  }

  // Highlight the first point (now the starting ELO point if present)
  const pointRadius = values.map((_, idx) => (idx === 0 ? 6 : 2));
  const pointBackgroundColor = values.map((_, idx) => (idx === 0 ? 'rgba(255, 215, 0, 1)' : 'rgba(59, 163, 255, 1)'));

  if (window.__fighterEloChart) window.__fighterEloChart.destroy();
  window.__fighterEloChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels,
      datasets: [
        {
          label: `${hist.name} — ELO`,
          data: values,
          borderColor: 'rgba(59, 163, 255, 1)',
          backgroundColor: 'rgba(59, 163, 255, 0.2)',
          tension: 0.2,
          pointRadius,
          pointBackgroundColor,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}`,
          },
        },
      },
      scales: { y: { beginAtZero: false } },
    },
  });
}

function renderCompareEloChart(histA, histB) {
  const ctx = document.getElementById('compareEloChart').getContext('2d');
  const datesA = (histA.points || []).map((p) => p.event_date || '').filter(Boolean);
  const datesB = (histB.points || []).map((p) => p.event_date || '').filter(Boolean);

  // Add a synthetic starting point one week before each fighter's first fight
  const preA = histA.entry_elo != null && datesA.length ? dateMinusDays(datesA[0], 7) : null;
  const preB = histB.entry_elo != null && datesB.length ? dateMinusDays(datesB[0], 7) : null;
  const allDates = Array.from(new Set([...datesA, ...datesB, ...(preA ? [preA] : []), ...(preB ? [preB] : [])])).sort();

  // Build smooth series: use values only on own dates (and pre-date), null elsewhere, and span gaps
  const byDateA = new Map((histA.points || []).map((p) => [p.event_date || '', p]));
  const byDateB = new Map((histB.points || []).map((p) => [p.event_date || '', p]));
  const valuesA = allDates.map((d) => {
    if (preA && d === preA) return histA.entry_elo;
    const p = byDateA.get(d);
    return p ? (p.elo_after ?? p.elo_before ?? null) : null;
  });
  const valuesB = allDates.map((d) => {
    if (preB && d === preB) return histB.entry_elo;
    const p = byDateB.get(d);
    return p ? (p.elo_after ?? p.elo_before ?? null) : null;
  });

  const startIdxA = preA ? allDates.indexOf(preA) : -1;
  const startIdxB = preB ? allDates.indexOf(preB) : -1;
  const radiiA = allDates.map((_, i) => (i === startIdxA && startIdxA !== -1 ? 6 : 2));
  const radiiB = allDates.map((_, i) => (i === startIdxB && startIdxB !== -1 ? 6 : 2));
  const pointBgA = allDates.map((_, i) => (i === startIdxA && startIdxA !== -1 ? 'rgba(255, 215, 0, 1)' : 'rgba(59, 163, 255, 1)'));
  const pointBgB = allDates.map((_, i) => (i === startIdxB && startIdxB !== -1 ? 'rgba(255, 215, 0, 1)' : 'rgba(255, 159, 64, 1)'));

  if (window.__compareEloChart) window.__compareEloChart.destroy();
  window.__compareEloChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: allDates,
      datasets: [
        {
          label: `${histA.name} — start ${histA.entry_elo ?? 'N/A'}`,
          data: valuesA,
          borderColor: 'rgba(59, 163, 255, 1)',
          backgroundColor: 'rgba(59, 163, 255, 0.2)',
          tension: 0.35,
          pointRadius: radiiA,
          pointBackgroundColor: pointBgA,
          spanGaps: true,
        },
        {
          label: `${histB.name} — start ${histB.entry_elo ?? 'N/A'}`,
          data: valuesB,
          borderColor: 'rgba(255, 159, 64, 1)',
          backgroundColor: 'rgba(255, 159, 64, 0.2)',
          tension: 0.35,
          pointRadius: radiiB,
          pointBackgroundColor: pointBgB,
          spanGaps: true,
        },
      ],
    },
    options: {
      plugins: {
        legend: { display: true },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}`,
          },
        },
      },
      scales: { y: { beginAtZero: false } },
    },
  });
}

async function loadTopListsSameSize() { /* deprecated */ }

async function loadTopChartByMetric(metric) {
  const chartEl = document.getElementById('topEloChart'); if (!chartEl) return;
  showLoaderFor(chartEl, 'Loading…');
  try {
    const limit = 15;
    if (metric === 'current') {
      const raw = await fetchJSON(`/analytics/top-elo?limit=${limit}`);
      const items = (raw || []).map((r) => ({ fighter_id: r.fighter_id, name: r.name, value: r.current_elo ?? 0 }));
      renderTopEloChart(items, 'Current ELO', { bg: 'rgba(59, 163, 255, 0.6)', stroke: 'rgba(59, 163, 255, 1)' });
    } else if (metric === 'peak') {
      const raw = await fetchJSON(`/analytics/top-peak-elo?limit=${limit}`);
      const items = (raw || []).map((r) => ({ fighter_id: r.fighter_id, name: r.name, value: r.peak_elo ?? 0 }));
      renderTopEloChart(items, 'Peak ELO', { bg: 'rgba(168, 85, 247, 0.6)', stroke: 'rgba(168, 85, 247, 1)' });
    } else if (metric === 'gain') {
      const items = await fetchJSON(`/analytics/top-elo-gain?limit=${limit}`);
      renderTopEloChart(items, 'ELO gain', { bg: 'rgba(34, 197, 94, 0.6)', stroke: 'rgba(34, 197, 94, 1)' });
    } else if (metric === 'peak_gain') {
      const items = await fetchJSON(`/analytics/top-peak-elo-gain?limit=${limit}`);
      renderTopEloChart(items, 'Peak ELO gain', { bg: 'rgba(255, 159, 64, 0.6)', stroke: 'rgba(255, 159, 64, 1)' });
    }
  } catch (e) {
    console.error('Failed to load top chart', e);
  } finally {
    hideLoaderFor(chartEl);
  }
}

// loadTopPeak handled via loadTopListsSameSize

// Fighter timeline and loader removed from home page

window.addEventListener('DOMContentLoaded', () => {
  // Header filters
  const yearInput = document.getElementById('fltYear');
  if (yearInput) { try { yearInput.value = String(new Date().getFullYear()); } catch {} }
  const rerunAll = () => {
    const year = Number(document.getElementById('fltYear')?.value || new Date().getFullYear());
    const division = document.getElementById('fltDivision')?.value || 'all';
    const wnd = Number(document.getElementById('fltWindow')?.value || 90);
    loadKpis({ year, division, window_days: wnd });
    loadRankPreview({ year, division });
    loadMovers({ window_days: wnd });
    loadFormLeaders();
    loadLatestEvent();
    loadDivisionOverview({ division, window_days: wnd });
    loadEventHighlights({ window_days: wnd });
    // Quick H2H uses on-demand button
  };
  ['fltDivision','fltWindow','fltYear'].forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', rerunAll);
  });

  // Header quick search
  const viewBtn = document.getElementById('viewBtn');
  if (viewBtn) viewBtn.addEventListener('click', async () => {
    const q = (document.getElementById('qHeader')?.value || '').trim();
    if (!q) return;
    let fighterId = q;
    if (/\s/.test(q) || q.length < 8) {
      try {
        const results = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=5`);
        if (!results || results.length === 0) { alert('No fighters found'); return; }
        fighterId = results[0].fighter_id;
      } catch (e) { console.error('Search failed', e); alert('Search failed'); return; }
    }
    window.location = `/viz/fighter.html?id=${encodeURIComponent(fighterId)}`;
  });
  try { attachAutocomplete('qHeader'); } catch {}
  try { attachAutocomplete('qhA'); attachAutocomplete('qhB'); } catch {}

  // Movers tabs
  const tabG = document.getElementById('tabGains');
  const tabL = document.getElementById('tabLosses');
  if (tabG && tabL) {
    tabG.addEventListener('click', () => { document.getElementById('tblGains').style.display = 'table'; document.getElementById('tblLosses').style.display = 'none'; });
    tabL.addEventListener('click', () => { document.getElementById('tblGains').style.display = 'none'; document.getElementById('tblLosses').style.display = 'table'; });
  }

  // Quick H2H button
  const qhBtn = document.getElementById('qhGo');
  if (qhBtn) qhBtn.addEventListener('click', () => loadQuickH2H());

  // Initial load
  rerunAll();
});

// Multi-compare: dynamic list
window.__compareList = [];
function randomColor(idx = 0) {
  const hue = (idx * 47) % 360; // distribute hues
  return `hsl(${hue} 80% 55%)`;
}

async function resolveFighterId(q) {
  if (!q) return null;
  if (/\s/.test(q) || q.length < 8) {
    try {
      const results = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=5`);
      return results?.[0]?.fighter_id || null;
    } catch {
      return null;
    }
  }
  return q;
}

function attachAutocomplete(inputId) {
  const inp = document.getElementById(inputId);
  if (!inp) return;
  // Floating dropdown attached to body to avoid layout jumps
  const list = document.createElement('div');
  list.className = 'ac-list';
  list.style.cssText = 'position:fixed;left:0;top:0;width:0;background:#0d1218;border:1px solid #1f2937;border-top:none;max-height:240px;overflow:auto;border-radius:0 0 8px 8px;display:none;z-index:1000;pointer-events:none;box-shadow:0 8px 16px rgba(0,0,0,0.35)';
  document.body.appendChild(list);

  function positionList() {
    const r = inp.getBoundingClientRect();
    list.style.left = `${Math.round(r.left)}px`;
    list.style.top = `${Math.round(r.bottom)}px`;
    list.style.width = `${Math.round(r.width)}px`;
  }

  let timer = null;
  async function query(q) {
    if (!q || q.length < 2) { list.style.display = 'none'; list.style.pointerEvents='none'; list.innerHTML=''; return; }
    try {
      const items = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=6`);
      if (!items || !items.length) { list.style.display = 'none'; list.innerHTML=''; return; }
      list.innerHTML = items.map(it => `<div class="ac-item" data-id="${it.fighter_id}" style="padding:8px 10px;cursor:pointer;">${it.name || it.fighter_id}</div>`).join('');
      positionList();
      list.style.display = 'block';
      list.style.pointerEvents = 'auto';
    } catch { list.style.display = 'none'; }
  }

  inp.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(() => query(inp.value.trim()), 180); });
  inp.addEventListener('focus', () => { if (list.innerHTML) { positionList(); list.style.display = 'block'; list.style.pointerEvents='auto'; } });
  inp.addEventListener('blur', () => { setTimeout(()=>{ list.style.display='none'; list.style.pointerEvents='none'; }, 100); });
  window.addEventListener('resize', () => { if (list.style.display === 'block') positionList(); });
  window.addEventListener('scroll', () => { if (list.style.display === 'block') positionList(); }, true);
  document.addEventListener('click', (e) => { if (!list.contains(e.target) && e.target !== inp) { list.style.display = 'none'; list.style.pointerEvents='none'; } });
  list.addEventListener('mouseleave', () => { list.style.display = 'none'; list.style.pointerEvents='none'; });
  list.addEventListener('click', (e) => {
    const el = e.target.closest('.ac-item'); if (!el) return;
    inp.value = el.textContent || el.dataset.id || '';
    list.style.display = 'none';
  });
}

async function addCompareFighter() {
  const input = document.getElementById('compareAddInput');
  const q = (input?.value || '').trim();
  if (!q) return;
  const fid = await resolveFighterId(q);
  if (!fid) { alert('No fighter found'); return; }
  if (window.__compareList.find((x) => x.fighter_id === fid)) { input.value = ''; return; }
  try {
    const hist = await fetchJSON(`/analytics/fighter-elo/${encodeURIComponent(fid)}`);
    const color = randomColor(window.__compareList.length);
    window.__compareList.push({ id: fid, name: hist.name, hist, color });
    input.value = '';
    renderCompareChips();
    renderMultiCompareChart(window.__compareList.map((x) => x.hist), window.__compareList.map((x) => x.color));
  } catch (e) {
    console.error('Failed to add fighter', e);
    alert('Failed to load fighter history');
  }
}

function clearCompare() {
  window.__compareList = [];
  renderCompareChips();
  const ctx = document.getElementById('compareEloChart').getContext('2d');
  if (window.__compareEloChart) { window.__compareEloChart.destroy(); window.__compareEloChart = null; }
  window.__compareEloChart = new Chart(ctx, { type: 'line', data: { labels: [], datasets: [] }, options: { plugins: { legend: { display: true } } } });
}

function renderCompareChips() {
  const holder = document.getElementById('compareChips');
  if (!holder) return;
  holder.innerHTML = '';
  window.__compareList.forEach((x, idx) => {
    const chip = document.createElement('div');
    chip.style.cssText = `background:#111827;border:1px solid #1f2937;border-radius:999px;padding:4px 10px;display:flex;align-items:center;gap:8px;`;
    chip.innerHTML = `<span style="width:10px;height:10px;border-radius:50%;background:${x.color};display:inline-block"></span><a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(x.id)}">${x.name || x.id}</a>`;
    holder.appendChild(chip);
  });
}

function renderMultiCompareChart(hists, colors) {
  const ctx = document.getElementById('compareEloChart').getContext('2d');
  if (!hists || !hists.length) return;
  // Build unified date axis including synthetic starting points
  const allPre = [];
  const series = hists.map((h) => {
    const dates = (h.points || []).map((p) => p.event_date || '').filter(Boolean);
    const pre = h.entry_elo != null && dates.length ? dateMinusDays(dates[0], 7) : null;
    if (pre) allPre.push(pre);
    return { h, dates, pre };
  });
  const allDates = Array.from(new Set(series.flatMap((s) => [ ...(s.pre ? [s.pre] : []), ...s.dates ]))).sort();
  const datasets = series.map((s, i) => {
    const byDate = new Map((s.h.points || []).map((p) => [p.event_date || '', p]));
    const values = allDates.map((d) => {
      if (s.pre && d === s.pre) return s.h.entry_elo;
      const p = byDate.get(d);
      return p ? (p.elo_after ?? p.elo_before ?? null) : null;
    });
    const color = colors?.[i] || randomColor(i);
    const startIdx = s.pre ? allDates.indexOf(s.pre) : -1;
    return {
      label: `${s.h.name} — start ${s.h.entry_elo ?? 'N/A'}`,
      data: values,
      borderColor: color,
      backgroundColor: color.replace('hsl', 'hsla').replace('%)', '%, 0.2)'),
      tension: 0.35,
      spanGaps: true,
      pointRadius: allDates.map((_, idx) => (idx === startIdx ? 6 : 2)),
    };
  });
  if (window.__compareEloChart) window.__compareEloChart.destroy();
  window.__compareEloChart = new Chart(ctx, {
    type: 'line',
    data: { labels: allDates, datasets },
    options: { plugins: { legend: { display: true }, tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${Number(c.parsed.y).toFixed(2)}` } } }, scales: { y: { beginAtZero: false } } },
  });
}

function fmtDelta(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return '-';
  const s = n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2);
  return s;
}

function outcomeBadge(out) {
  if (out === 'W') return '<span class="badge win">W</span>';
  if (out === 'L') return '<span class="badge loss">L</span>';
  if (out === 'D') return '<span class="badge draw">D</span>';
  if (out === 'NC') return '<span class="badge">NC</span>';
  return out || '-';
}

function renderChangeTable(tableId, items) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  if (!tbody) return;
  const rows = (items || []).map((r) => {
    const fighter = r.fighter_name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter_id)}">${r.fighter_name}</a>` : (r.fighter_id || '-');
    const opp = r.opponent_name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.opponent_id)}">${r.opponent_name}</a>` : (r.opponent_id || '-');
    const evText = r.event_name ? `${r.event_name}${r.event_date ? ` (${r.event_date})` : ''}` : (r.event_date || '-');
    const evCell = `${evText}`;
    const delta = Number(r.delta);
    const deltaCell = `<span class="delta ${delta >= 0 ? 'pos' : 'neg'}">${fmtDelta(delta)}</span>`;
    const outcomeCell = outcomeBadge(r.outcome);
    const boutCell = r.bout_id ? `<a class=\"link-muted\" href=\"/viz/bout.html?bout_id=${encodeURIComponent(r.bout_id)}\" title=\"Open bout\">Open</a>` : '-';
    return `<tr>
      <td>${fighter}</td>
      <td>${opp}</td>
      <td>${deltaCell}</td>
      <td>${outcomeCell}</td>
      <td>${evCell}</td>
      <td>${boutCell}</td>
    </tr>`;
  }).join('');
  tbody.innerHTML = rows;
}

async function loadChangeLeaderboards() {
  const tbl1 = document.getElementById('tblHighGains'); if (tbl1) showLoaderFor(tbl1, 'Loading gains…');
  const tbl2 = document.getElementById('tblLowGains'); if (tbl2) showLoaderFor(tbl2, 'Loading low gains…');
  const tbl3 = document.getElementById('tblHighLosses'); if (tbl3) showLoaderFor(tbl3, 'Loading losses…');
  try {
    const limitHigh = Number(window.__lbCounts?.highG || 10);
    const limitLow = Number(window.__lbCounts?.lowG || 10);
    const limitLoss = Number(window.__lbCounts?.highL || 10);
    const [highG, lowG, highL] = await Promise.all([
      fetchJSON(`/analytics/top-elo-gains?limit=${limitHigh}`),
      fetchJSON(`/analytics/lowest-elo-gains?limit=${limitLow}`),
      fetchJSON(`/analytics/top-elo-losses?limit=${limitLoss}`),
    ]);
    renderChangeTable('tblHighGains', highG);
    renderChangeTable('tblLowGains', lowG);
    renderChangeTable('tblHighLosses', highL);
  } catch (e) {
    console.error('Failed to load elo change leaderboards', e);
  } finally {
    if (tbl1) hideLoaderFor(tbl1);
    if (tbl2) hideLoaderFor(tbl2);
    if (tbl3) hideLoaderFor(tbl3);
  }
}

// Random bouts module removed per new dashboard design (no calls)

// Rankings history
async function loadRankingsHistory() {
  const tbl = document.getElementById('tblRankingsYear'); if (tbl) showLoaderFor(tbl, 'Loading rankings…');
  try {
    const snaps = await fetchJSON('/analytics/rankings-history?top=15');
    window.__rankingsSnapshots = snaps || [];
    const years = (window.__rankingsSnapshots || []).map((s) => Number(s.label)).filter((x) => !Number.isNaN(x));
    if (!years.length) return;
    const minY = Math.min(...years), maxY = Math.max(...years);
    const slider = document.getElementById('rankYear');
    const lab = document.getElementById('rankYearLabel');
    if (slider) { slider.min = String(minY); slider.max = String(maxY); slider.value = String(maxY); }
    if (lab) lab.textContent = String(maxY);
    renderRankingsForYear(maxY);
  } catch (e) {
    console.error('Failed to load rankings history', e);
  } finally { if (tbl) hideLoaderFor(tbl); }
}

// ---- Latest event summary (home) ----
let __eventsList = null;
let __eventIdx = -1;

function renderLatestEventRows(data, limit) {
  const entries = Array.isArray(data?.entries) ? data.entries : [];
  const rows = entries.slice(0, limit).map((r) => {
    const f1n = r.fighter1_name || r.fighter1_id || '-';
    const f2n = r.fighter2_name || r.fighter2_id || '-';
    const boutText = `${f1n} vs ${f2n}`;
    const outText = `${r.fighter1_outcome || '-'} / ${r.fighter2_outcome || '-'}`;
    const d1 = Number.isFinite(Number(r.fighter1_delta)) ? Number(r.fighter1_delta) : null;
    const d2 = Number.isFinite(Number(r.fighter2_delta)) ? Number(r.fighter2_delta) : null;
    const deltaCell = `${d1 != null ? `<span class="delta ${d1 >= 0 ? 'pos' : 'neg'}">${fmtDelta(d1)}</span>` : '-'} / ${d2 != null ? `<span class="delta ${d2 >= 0 ? 'pos' : 'neg'}">${fmtDelta(d2)}</span>` : '-'}`;
    const open = r.bout_id ? `<a class="link-muted" href="/viz/bout.html?bout_id=${encodeURIComponent(r.bout_id)}">Open</a>` : '-';
    return `<tr>
      <td>${boutText}</td>
      <td>${outText}</td>
      <td>${deltaCell}</td>
      <td>${open}</td>
    </tr>`;
  }).join('');
  return rows;
}

async function loadLatestEventSummaryHome() {
  const tbl = document.getElementById('tblLatestEventHome'); if (tbl) showLoaderFor(tbl, 'Loading latest event…');
  try {
    // Load events list once
    if (!__eventsList) {
      __eventsList = await fetchJSON('/analytics/events');
      // sort ascending by date; assume backend already does; ensure anyway
      __eventsList.sort((a,b) => String(a.event_date||'') < String(b.event_date||'') ? -1 : 1);
    }
    // default to latest (last element with a date)
    __eventIdx = __eventsList.length - 1;
    await renderHomeEventByIndex(__eventIdx);
    const prevBtn = document.getElementById('btnPrevEventHome');
    const nextBtn = document.getElementById('btnNextEventHome');
    if (prevBtn) prevBtn.addEventListener('click', async () => { if (__eventIdx > 0) { __eventIdx -= 1; await renderHomeEventByIndex(__eventIdx); } });
    if (nextBtn) nextBtn.addEventListener('click', async () => { if (__eventIdx < __eventsList.length - 1) { __eventIdx += 1; await renderHomeEventByIndex(__eventIdx); } });
  } catch (e) {
    console.error('Failed to load latest event for home', e);
  } finally { if (tbl) hideLoaderFor(tbl); }
}

async function renderHomeEventByIndex(idx) {
  const meta = document.getElementById('eventMetaHome');
  const tbody = document.querySelector('#tblLatestEventHome tbody');
  if (!__eventsList || idx < 0 || idx >= __eventsList.length) return;
  const ev = __eventsList[idx];
  let data;
  try {
    data = await fetchJSON(`/analytics/event-elo?event_id=${encodeURIComponent(ev.event_id)}`);
  } catch (e) {
    console.warn('event-elo failed; fallback latest', e);
    data = await fetchJSON('/analytics/latest-event-elo');
  }
  if (meta) meta.innerHTML = `<a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(ev.event_id)}">${ev.name || 'Event'}${ev.event_date ? ` (${ev.event_date})` : ''}</a>`;
  if (tbody) tbody.innerHTML = renderLatestEventRows(data, 5);
}

function renderRankingsForYear(y) {
  const lab = document.getElementById('rankYearLabel'); if (lab) lab.textContent = String(y);
  const tbody = document.querySelector('#tblRankingsYear tbody');
  if (!tbody) return;
  const headRow = document.getElementById('tblRankingsYearHead');
  const sel = document.getElementById('rankMetric');
  const full = document.getElementById('rankFull');
  const mode = sel ? sel.value : 'elo';
  const wantFull = full ? full.checked : false;

  async function ensureFullSnapshots() {
    const cur = (window.__rankingsSnapshots || []).find((s) => Number(s.label) === Number(y));
    if (!wantFull) return cur;
    const allSnaps = await fetchJSON('/analytics/rankings-history?top=1000');
    window.__rankingsSnapshots = allSnaps || window.__rankingsSnapshots;
    return (window.__rankingsSnapshots || []).find((s) => Number(s.label) === Number(y));
  }

  async function fetchYearlyGains() {
    const limit = wantFull ? 1000 : 10;
    return await fetchJSON(`/analytics/yearly-elo-gains?year=${encodeURIComponent(y)}&limit=${limit}`);
  }

  (async () => {
    if (mode === 'gains') {
      const tbl = document.getElementById('tblRankingsYear'); if (tbl) showLoaderFor(tbl, 'Loading yearly gains…');
      if (headRow) headRow.innerHTML = '<th>Rank</th><th>Fighter</th><th>Record</th><th>Δ ELO</th>';
      const items = await fetchYearlyGains();
      const rows = (items || []).map((e, idx) => {
        const rank = idx + 1;
        const nameCell = e.name
          ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(e.fighter_id)}">${e.name}</a>`
          : (e.fighter_id || '-');
        const w = Number(e.wins ?? 0), l = Number(e.losses ?? 0), d = Number(e.draws ?? 0);
        const fights = Number(e.fights ?? (w + l + d));
        const recCell = `${w}-${l}${d ? `-${d}` : ''}${fights ? ` (${fights})` : ''}`;
        const deltaCell = Number.isFinite(Number(e.delta)) ? Number(e.delta).toFixed(2) : '-';
        let clazz = '';
        if (rank === 1) clazz = 'podium-gold'; else if (rank === 2) clazz = 'podium-silver'; else if (rank === 3) clazz = 'podium-bronze';
        return `<tr class="${clazz}">
          <td>${rank}</td>
          <td>${nameCell}</td>
          <td>${recCell}</td>
          <td style="text-align:right; font-variant-numeric: tabular-nums;" class="${Number(e.delta) >= 0 ? 'delta pos' : 'delta neg'}">${deltaCell}</td>
        </tr>`;
      }).join('');
      tbody.innerHTML = rows;
      if (tbl) hideLoaderFor(tbl);
      return;
    }
    // ELO mode
    const tbl = document.getElementById('tblRankingsYear'); if (tbl) showLoaderFor(tbl, 'Loading yearly ELO…');
    if (headRow) headRow.innerHTML = '<th>Rank</th><th>Fighter</th><th>Record</th><th>ELO</th>';
    const snap = await ensureFullSnapshots();
    if (!snap) { tbody.innerHTML = ''; return; }
    const entries = wantFull ? (snap.entries || []) : (snap.entries || []).slice(0, 10);
    const rows = entries.map((e) => {
      const rank = e.rank ?? '-';
      const nameCell = e.name
        ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(e.fighter_id)}">${e.name}</a>`
        : (e.fighter_id || '-');
      const w = Number(e.wins ?? 0), l = Number(e.losses ?? 0), d = Number(e.draws ?? 0);
      const fights = Number(e.fights ?? (w + l + d));
      const recCell = `${w}-${l}${d ? `-${d}` : ''}${fights ? ` (${fights})` : ''}`;
      const elo = Number(e.elo);
      const eloCell = Number.isFinite(elo) ? elo.toFixed(2) : '-';
      let clazz = '';
      if (rank === 1) clazz = 'podium-gold'; else if (rank === 2) clazz = 'podium-silver'; else if (rank === 3) clazz = 'podium-bronze';
      return `<tr class="${clazz}">
        <td>${rank}</td>
        <td>${nameCell}</td>
        <td>${recCell}</td>
        <td style="text-align:right; font-variant-numeric: tabular-nums;">${eloCell}</td>
      </tr>`;
    }).join('');
    tbody.innerHTML = rows;
    if (tbl) hideLoaderFor(tbl);
  })();
}

// ---- New Home Dashboard helpers ----

function kpiChip(label, value, href) {
  const aOpen = href ? `<a class="link-muted" href="${href}">` : '';
  const aClose = href ? '</a>' : '';
  return `<div class="kpi-chip">
    <div class="kpi-chip-title">${label}</div>
    <div class="kpi-chip-value">${aOpen}${value}${aClose}</div>
  </div>`;
}

async function loadKpis({ year, division, window_days }) {
  const host = document.getElementById('kpiRow'); if (!host) return;
  showLoaderFor(host, 'Loading KPIs…');
  try {
    const gainsP = fetchJSON(`/analytics/elo-movers?direction=gains&limit=1${window_days ? `&window_days=${window_days}` : ''}`);
    const latestEvP = fetchJSON('/analytics/latest-event-elo').catch(() => null);
    const rankP = fetchJSON(`/analytics/rankings-year?year=${encodeURIComponent(year)}&top=1${division && division !== 'all' ? `&division=${encodeURIComponent(division)}` : ''}`);
    const [gains, latestEv, ranks] = await Promise.all([gainsP, latestEvP, rankP]);
    const topG = Array.isArray(gains) && gains[0] ? gains[0] : null;
    const shock = latestEv?.shock_index != null ? Number(latestEv.shock_index).toFixed(2) : '—';
    const netTransfer = latestEv?.net_transfer != null ? Number(latestEv.net_transfer).toFixed(1) : '—';
    const latestEventLabel = latestEv?.event_name ? `${latestEv.event_name}${latestEv.event_date ? ` (${latestEv.event_date})` : ''}` : '—';
    const topRank = ranks?.entries && ranks.entries[0] ? ranks.entries[0] : null;
    const divisionLabel = division && division !== 'all' ? formatDivision(division) : 'All divisions';
    const chips = [
      kpiChip('Top gainer', topG ? `${topG.fighter_name || topG.fighter_id} +${Number(topG.delta ?? 0).toFixed(2)}` : '—', topG ? `/viz/fighter.html?id=${encodeURIComponent(topG.fighter_id)}` : undefined),
      kpiChip('Latest event', latestEventLabel, latestEv?.event_id ? `/viz/event_insights.html?event_id=${encodeURIComponent(latestEv.event_id)}` : undefined),
      kpiChip('Shock index', shock, latestEv?.event_id ? `/viz/event_insights.html?event_id=${encodeURIComponent(latestEv.event_id)}` : undefined),
      kpiChip('Net ELO transfer', netTransfer, latestEv?.event_id ? `/viz/event_insights.html?event_id=${encodeURIComponent(latestEv.event_id)}` : undefined),
      kpiChip('Rank #1', topRank ? `${topRank.name || topRank.fighter_id} ${Number(topRank.elo ?? topRank.current_elo ?? 0).toFixed(1)}` : '—', topRank ? `/viz/fighter.html?id=${encodeURIComponent(topRank.fighter_id)}` : undefined),
      kpiChip('Division filter', divisionLabel),
    ].join('');
    host.innerHTML = chips;
  } catch (e) {
    console.error('KPIs load failed', e);
    host.innerHTML = '<small style="color:var(--muted);">Failed to load KPIs.</small>';
  } finally { hideLoaderFor(host); }
}

async function loadRankPreview({ year, division }) {
  const tbl = document.getElementById('tblRankPreview'); if (!tbl) return;
  showLoaderFor(tbl, 'Loading rankings…');
  try {
    const data = await fetchJSON(`/analytics/rankings-year?year=${encodeURIComponent(year)}&top=10&offset=0${division && division !== 'all' ? `&division=${encodeURIComponent(division)}` : ''}`);
    const rows = (data?.entries || data?.rows || []).slice(0, 10).map((r) => {
      const rank = r.rank ?? '-';
      const name = r.name || r.fighter_name || r.fighter_id;
      const elo = Number(r.elo ?? r.current_elo ?? 0).toFixed(2);
      const dy = r.delta_yoy != null ? Number(r.delta_yoy).toFixed(2) : '—';
      const div = formatDivision(r.division ?? r.division_code);
      return `<tr>
        <td>${rank}</td>
        <td><a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter_id)}">${name}</a></td>
        <td style="text-align:right; font-variant-numeric: tabular-nums;">${elo}</td>
        <td style="text-align:right; font-variant-numeric: tabular-nums;">${dy}</td>
        <td>${div}</td>
      </tr>`;
    }).join('');
    tbl.querySelector('tbody').innerHTML = rows;
  } catch (e) {
    console.error('Rank preview failed', e);
  } finally { hideLoaderFor(tbl); }
}

async function loadMovers({ window_days }) {
  const tblG = document.getElementById('tblGains'); const tblL = document.getElementById('tblLosses');
  if (tblG) showLoaderFor(tblG, 'Loading gains…'); if (tblL) showLoaderFor(tblL, 'Loading losses…');
  try {
    const qs = window_days ? `&window_days=${window_days}` : '';
    const [gains, losses] = await Promise.all([
      fetchJSON(`/analytics/elo-movers?direction=gains&limit=10${qs}`),
      fetchJSON(`/analytics/elo-movers?direction=losses&limit=10${qs}`),
    ]);
    const mapRow = (r) => {
      const name = r.fighter_name || r.name || r.fighter_id;
      const delta = Number(r.delta ?? 0).toFixed(2);
      const fights = r.fights ?? r.fight_count ?? '—';
      const last = r.last_event_date || r.last_fight_date || r.event_date || '';
      const eventLink = r.last_event_id ? `<a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(r.last_event_id)}">${last || 'Event'}</a>` : (last || '—');
      return `<tr>
        <td><a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter_id)}">${name}</a></td>
        <td style="text-align:right; font-variant-numeric: tabular-nums;">${delta}</td>
        <td>${fights}</td>
        <td>${eventLink}</td>
      </tr>`;
    };
    if (tblG) tblG.querySelector('tbody').innerHTML = (gains || []).map(mapRow).join('');
    if (tblL) tblL.querySelector('tbody').innerHTML = (losses || []).map(mapRow).join('');
  } catch (e) {
    console.error('Movers failed', e);
  } finally { if (tblG) hideLoaderFor(tblG); if (tblL) hideLoaderFor(tblL); }
}

async function loadFormLeaders() {
  const tbl = document.getElementById('tblForm'); if (!tbl) return;
  showLoaderFor(tbl, 'Loading form leaders…');
  try {
    const params = new URLSearchParams({
      window: 'fights',
      n: '5',
      top: '10',
      min_recent_fights: '2',
      recent_days: '730',
    });
    const data = await fetchJSON(`/analytics/form-top?${params.toString()}`);
    const rows = (data || []).map((r, idx) => {
      const name = r.fighter_name || r.name || r.fighter_id || '—';
      const fi = Number(r.fi ?? r.form ?? 0).toFixed(3);
      const count = r.recent_fights ?? r.count ?? '—';
      const last = r.last_event_date || r.last_fight_date || '—';
      const link = `/viz/fighter.html?id=${encodeURIComponent(r.fighter_id)}`;
      return `<tr>
        <td>${idx + 1}. <a class="link-muted" href="${link}">${name}</a></td>
        <td style="text-align:right; font-variant-numeric: tabular-nums;">${fi}</td>
        <td style="text-align:center;">${count}</td>
        <td>${last}</td>
      </tr>`;
    }).join('');
    tbl.querySelector('tbody').innerHTML = rows || '<tr><td colspan="4" style="text-align:center; color:var(--muted);">No data</td></tr>';
  } catch (e) {
    console.error('Form leaders failed', e);
  } finally { hideLoaderFor(tbl); }
}

async function loadLatestEvent() {
  const hdr = document.getElementById('latestEventHeader');
  const chips = document.getElementById('latestEventChips');
  const tbl = document.getElementById('tblLatestEventHome');
  if (tbl) showLoaderFor(tbl, 'Loading latest event…');
  try {
    const data = await fetchJSON('/analytics/latest-event-elo');
    if (hdr) hdr.innerHTML = `<a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(data.event_id)}">${data.event_name || 'Event'}${data.event_date ? ` (${data.event_date})` : ''}</a>`;
    if (chips) chips.innerHTML = `<span class="badge">Shock ${Number(data.shock_index ?? 0).toFixed(2)}</span><span class="badge">Title bouts ${data.title_bouts ?? 0}</span>`;
    if (tbl) {
      const rows = (data.rows || data.entries || []).map((r) => {
        const bout = `${r.fighter1_name || r.fighter1_id} vs ${r.fighter2_name || r.fighter2_id}`;
        const method = r.method || '-';
        const winnerDelta = r.fighter1_outcome === 'W' ? r.fighter1_delta : (r.fighter2_outcome === 'W' ? r.fighter2_delta : null);
        const loserDelta = r.fighter1_outcome === 'L' ? r.fighter1_delta : (r.fighter2_outcome === 'L' ? r.fighter2_delta : null);
        return `<tr>
          <td>${bout}</td>
          <td>${method}</td>
          <td style="text-align:right; font-variant-numeric: tabular-nums;">${winnerDelta != null ? Number(winnerDelta).toFixed(2) : '—'}</td>
          <td style="text-align:right; font-variant-numeric: tabular-nums;">${loserDelta != null ? Number(loserDelta).toFixed(2) : '—'}</td>
        </tr>`;
      }).join('');
      tbl.querySelector('tbody').innerHTML = rows;
    }
  } catch (e) {
    console.error('Latest event failed', e);
  } finally { if (tbl) hideLoaderFor(tbl); }
}

async function loadDivisionOverview({ division, window_days }) {
  const grid = document.getElementById('divGrid'); if (!grid) return;
  showLoaderFor(grid, 'Loading divisions…');
  try {
    const codes = division && division !== 'all' ? [Number(division)] : MEN_DIVISION_CODES;
    const results = await Promise.all(
      codes.map((c) =>
        fetchJSON(`/analytics/division?code=${encodeURIComponent(c)}&top=3`)
          .then((r) => ({ code: c, data: r }))
          .catch(() => ({ code: c, data: null }))
      )
    );
    grid.innerHTML = results.map(({ code, data }) => {
      const rows = data?.rows || [];
      const title = data?.division_name || formatDivision(code);
      const active = data?.active_count ?? rows.length;
      const items = rows.slice(0, 3).map((r, idx) => {
        const eloVal = Number(r.elo ?? r.current_elo ?? 0).toFixed(0);
        const deltaRaw = Number(r.delta_recent ?? 0);
        const deltaSpan = Number.isFinite(deltaRaw) && deltaRaw !== 0
          ? `<span class=\"${deltaRaw >= 0 ? 'delta pos' : 'delta neg'}\" style=\"margin-left:8px;\">${fmtDelta(deltaRaw)}</span>`
          : '';
        const progressRaw = Number(r.elo_progress ?? ((r.elo ?? r.current_elo ?? 0) - (r.entry_elo ?? r.entry ?? 0)));
        const progressLine = Number.isFinite(progressRaw) && progressRaw !== 0
          ? `<small style=\"color:${progressRaw >= 0 ? 'var(--positive)' : 'var(--negative)'}; display:block;\">Net Δ since entry: ${fmtDelta(progressRaw)}</small>`
          : '';
        const lastDate = r.last_event_date ? `<small style=\"color:var(--muted); display:block;\">Last: ${r.last_event_date}</small>` : '';
        return `<div style=\"margin-bottom:6px;\">
          ${idx + 1}. <a class=\"link-muted\" href=\"/viz/fighter.html?id=${encodeURIComponent(r.fighter_id)}\">${r.fighter_name || r.name || r.fighter_id}</a>
          <span style=\"float:right; font-variant-numeric: tabular-nums;\">${eloVal}</span>
          ${deltaSpan}
          ${progressLine}
          ${lastDate}
        </div>`;
      }).join('');
      return `<div class=\"card\" style=\"padding:12px;\">
        <div class=\"title\" style=\"margin-bottom:6px;\">${title}</div>
        <small style=\"color:var(--muted); display:block; margin-bottom:8px;\">Active fighters: ${active}</small>
        ${items || '<small style=\"color:var(--muted);\">No data</small>'}
      </div>`;
    }).join('');
  } catch (e) {
    console.error('Division overview failed', e);
  } finally { hideLoaderFor(grid); }
}

async function loadEventHighlights({ window_days }) {
  const tblShock = document.getElementById('tblShock');
  const tblChalk = document.getElementById('tblChalk');
  if (tblShock) showLoaderFor(tblShock, 'Loading shocking…');
  if (tblChalk) showLoaderFor(tblChalk, 'Loading predictable…');
  try {
    const normalizeRange = (value) => {
      const num = Number(value);
      if (!Number.isFinite(num) || num <= 0) return '';
      return `${Math.trunc(num)}d`;
    };
    const rangeStr = normalizeRange(window_days);
    const shockParams = new URLSearchParams({ type: 'shocking', limit: '5' });
    const chalkParams = new URLSearchParams({ type: 'predictable', limit: '5' });
    if (rangeStr) {
      shockParams.set('range', rangeStr);
      chalkParams.set('range', rangeStr);
    }
    const [shock, chalk] = await Promise.all([
      fetchJSON(`/analytics/events-shock-top?${shockParams.toString()}`),
      fetchJSON(`/analytics/events-shock-top?${chalkParams.toString()}`),
    ]);
    const mapRow = (r) => `<tr>
      <td><a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(r.event_id)}">${r.event_name || r.name || r.event_id}${r.event_date ? ` (${r.event_date})` : ''}</a></td>
      <td style="text-align:right; font-variant-numeric: tabular-nums;">${Number(r.shock ?? r.shock_index ?? r.index ?? 0).toFixed(2)}</td>
    </tr>`;
    if (tblShock) tblShock.querySelector('tbody').innerHTML = (shock || []).map(mapRow).join('');
    if (tblChalk) tblChalk.querySelector('tbody').innerHTML = (chalk || []).map(mapRow).join('');
  } catch (e) {
    console.error('Event highlights failed', e);
  } finally { if (tblShock) hideLoaderFor(tblShock); if (tblChalk) hideLoaderFor(tblChalk); }
}

async function loadQuickH2H() {
  const a = (document.getElementById('qhA')?.value || '').trim();
  const b = (document.getElementById('qhB')?.value || '').trim();
  const out = document.getElementById('qhOut'); if (!out || !a || !b) return;
  showLoaderFor(out, 'Calculating…');
  try {
    const data = await fetchJSON(`/analytics/h2h?fighter1=${encodeURIComponent(a)}&fighter2=${encodeURIComponent(b)}&mode1=current&mode2=current&adjust=base&explain=false`);
    const pa = Number(data?.p1_win ?? data?.pA ?? data?.prob1 ?? 0);
    const pb = Number(data?.p2_win ?? data?.pB ?? data?.prob2 ?? (1 - pa));
    out.innerHTML = `<div>Win prob: <strong>${(pa*100).toFixed(1)}%</strong> vs <strong>${(pb*100).toFixed(1)}%</strong></div>`;
  } catch (e) {
    console.error('Quick H2H failed', e);
    out.innerHTML = '<small style="color:var(--muted);">Calculation failed.</small>';
  } finally { hideLoaderFor(out); }
}
