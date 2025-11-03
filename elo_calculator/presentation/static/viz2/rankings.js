async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  return payload.data !== undefined ? payload.data : payload;
}

function showLoaderFor(el, text = 'Loading…') {
  const card = el.closest('.card') || el;
  const ex = card.querySelector(':scope > .loader-overlay');
  if (ex) return ex;
  const overlay = document.createElement('div');
  overlay.className = 'loader-overlay';
  overlay.innerHTML = `<div class="loader"><span class="spinner"></span><span>${text}</span></div>`;
  card.appendChild(overlay);
  return overlay;
}
function hideLoaderFor(el) { const card = el.closest('.card') || el; const ov = card.querySelector(':scope > .loader-overlay'); if (ov) ov.remove(); }

window.addEventListener('DOMContentLoaded', () => {
  const slider = document.getElementById('rankYear');
  const sel = document.getElementById('rankMetric');
  const chk = document.getElementById('rankFull');
  const btnFirst = document.getElementById('btnFirstYear');
  const btnPrev = document.getElementById('btnPrevYear');
  const btnNext = document.getElementById('btnNextYear');
  const btnLast = document.getElementById('btnLastYear');
  const btnMore = document.getElementById('btnViewMore');
  const yearSelect = document.getElementById('rankYearSelect');

  // Visible count state (increments of 10)
  window.__rankPageSize = 10;
  window.__rankOffset = 0;

  const onChange = () => {
    const y = Number(slider.value);
    if (yearSelect) yearSelect.value = String(y);
    renderRankingsForYear(y);
  };
  if (slider) slider.addEventListener('input', onChange);
  if (yearSelect) yearSelect.addEventListener('change', () => {
    const y = Number(yearSelect.value);
    if (slider) slider.value = String(y);
    renderRankingsForYear(y);
  });
  if (sel) sel.addEventListener('change', onChange);
  if (chk) chk.addEventListener('change', onChange);
  if (btnFirst) btnFirst.addEventListener('click', () => { slider.value = slider.min; onChange(); });
  if (btnPrev) btnPrev.addEventListener('click', () => { slider.value = String(Math.max(Number(slider.min), Number(slider.value) - 1)); onChange(); });
  if (btnNext) btnNext.addEventListener('click', () => { slider.value = String(Math.min(Number(slider.max), Number(slider.value) + 1)); onChange(); });
  if (btnLast) btnLast.addEventListener('click', () => { slider.value = slider.max; onChange(); });
  if (btnMore) btnMore.addEventListener('click', async () => {
    const year = Number(document.getElementById('rankYear')?.value || 0);
    const size = Number(window.__rankPageSize || 10);
    const offset = Number(window.__rankOffset || 0) + size;
    const sel = document.getElementById('rankMetric');
    const mode = sel ? sel.value : 'elo';
    const tbl = document.getElementById('tblRankingsYear'); if (tbl) showLoaderFor(tbl, mode === 'gains' ? 'Loading yearly gains…' : 'Loading yearly ELO…');
    try {
      if (mode === 'gains') {
        const items = await fetchJSON(`/analytics/yearly-elo-gains?year=${encodeURIComponent(year)}&offset=${offset}&page_size=${size}`);
        appendRows(items.map((e, idx) => rowFor(offset + idx + 1, e.name, e.fighter_id, e.wins, e.losses, e.draws, e.fights, e.delta, true)).join(''));
      } else {
        const snap = await fetchJSON(`/analytics/rankings-year?year=${encodeURIComponent(year)}&offset=${offset}&page_size=${size}`);
        appendRows((snap.entries || []).map((e) => rowFor(e.rank, e.name, e.fighter_id, e.wins, e.losses, e.draws, e.fights, e.elo, false)).join(''));
      }
      window.__rankOffset = offset;
    } finally { if (tbl) hideLoaderFor(tbl); }
  });

  loadYears().then((y) => { if (y) renderRankingsForYear(y.max); });
});

async function loadYears() {
  const tbl = document.getElementById('tblRankingsYear');
  if (tbl) showLoaderFor(tbl, 'Loading rankings…');
  try {
    const years = await fetchJSON('/analytics/ranking-years');
    if (!years.length) return null;
    const slider = document.getElementById('rankYear');
    const lab = document.getElementById('rankYearLabel');
    if (slider) { slider.min = String(years[0]); slider.max = String(years[years.length - 1]); slider.value = String(years[years.length - 1]); }
    const yearSelect = document.getElementById('rankYearSelect');
    if (yearSelect) {
      yearSelect.innerHTML = years.map((y) => `<option value="${y}">${y}</option>`).join('');
      yearSelect.value = String(years[years.length - 1]);
    }
    if (lab) lab.textContent = String(years[years.length - 1]);
    return { min: years[0], max: years[years.length - 1] };
  } finally { if (tbl) hideLoaderFor(tbl); }
}

async function renderRankingsForYear(y) {
  const lab = document.getElementById('rankYearLabel'); if (lab) lab.textContent = String(y);
  const tbody = document.querySelector('#tblRankingsYear tbody'); if (!tbody) return;
  // Reset pagination on year change
  window.__rankOffset = 0;
  const headRow = document.getElementById('tblRankingsYearHead');
  const sel = document.getElementById('rankMetric');
  const mode = sel ? sel.value : 'elo';
  const size = Math.max(10, Number(window.__rankPageSize || 10));
  const tbl = document.getElementById('tblRankingsYear'); if (tbl) showLoaderFor(tbl, mode === 'gains' ? 'Loading yearly gains…' : 'Loading yearly ELO…');

  try {
    if (mode === 'gains') {
      if (headRow) headRow.innerHTML = '<th>Rank</th><th>Fighter</th><th>Record</th><th>Δ ELO</th>';
      const items = await fetchJSON(`/analytics/yearly-elo-gains?year=${encodeURIComponent(y)}&offset=0&page_size=${size}`);
      tbody.innerHTML = (items || []).map((e, idx) => rowFor(idx + 1, e.name, e.fighter_id, e.wins, e.losses, e.draws, e.fights, e.delta, true)).join('');
      return;
    }
    if (headRow) headRow.innerHTML = '<th>Rank</th><th>Fighter</th><th>Record</th><th>ELO</th>';
    const snap = await fetchJSON(`/analytics/rankings-year?year=${encodeURIComponent(y)}&offset=0&page_size=${size}`);
    const entries = (snap.entries || []);
    tbody.innerHTML = entries.map((e) => rowFor(e.rank, e.name, e.fighter_id, e.wins, e.losses, e.draws, e.fights, e.elo, false)).join('');
  } finally { if (tbl) hideLoaderFor(tbl); }
}

function rowFor(rank, name, id, w=0, l=0, d=0, fights=0, num=null, isDelta=false) {
  const link = name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(id)}">${name}</a>` : (id || '-');
  const rec = `${Number(w)}-${Number(l)}${d ? `-${Number(d)}` : ''}${fights ? ` (${Number(fights)})` : ''}`;
  const val = (num != null && Number.isFinite(Number(num))) ? Number(num).toFixed(2) : '-';
  let clazz = '';
  if (rank === 1) clazz = 'podium-gold'; else if (rank === 2) clazz = 'podium-silver'; else if (rank === 3) clazz = 'podium-bronze';
  const valClass = isDelta ? (Number(num) >= 0 ? 'delta pos' : 'delta neg') : '';
  return `<tr class="${clazz}">\n    <td>${rank ?? '-'}</td>\n    <td>${link}</td>\n    <td>${rec}</td>\n    <td style="text-align:right; font-variant-numeric: tabular-nums;" class="${valClass}">${val}</td>\n  </tr>`;
}

function appendRows(html) {
  const tbody = document.querySelector('#tblRankingsYear tbody'); if (!tbody) return;
  const tmp = document.createElement('tbody');
  tmp.innerHTML = html;
  [...tmp.children].forEach((tr) => tbody.appendChild(tr));
}
