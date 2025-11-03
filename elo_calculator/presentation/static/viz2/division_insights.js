/* global document, fetch, alert */

async function fetchJSON(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data && data.message) || `Request failed ${res.status}`);
  return data.data ?? data;
}
function q(sel){ return document.querySelector(sel); }
function fmt2(x){ if (x === null || x === undefined) return '—'; return Number(x).toFixed(3); }

async function loadDivisions(){
  const items = await fetchJSON('/analytics/divisions');
  const sel = q('#divSelect');
  sel.innerHTML = items.map(it => `<option value="${it.code}">${it.label}</option>`).join('');
}

async function loadParityAndChurn(){
  const div = q('#divSelect')?.value;
  const yearRaw = q('#yearInput')?.value?.trim();
  const year = yearRaw ? Number(yearRaw) : null;
  const parity = await fetchJSON(`/analytics/division-parity?division=${encodeURIComponent(div)}${year ? `&year=${year}` : ''}`);
  q('#parityBlock').innerHTML = `<div>Gini: <strong>${fmt2(parity.gini)}</strong> (N=${parity.count})</div>`;
  if (year) {
    const churn = await fetchJSON(`/analytics/division-churn?division=${encodeURIComponent(div)}&year=${year}`);
    const t = churn.top10_turnover != null ? (churn.top10_turnover * 100).toFixed(1) + '%' : '—';
    q('#churnBlock').innerHTML = `<div>Avg |ΔELO| per participation: <strong>${fmt2(churn.avg_abs_delta)}</strong> • Top‑10 unchanged: ${churn.top10_unchanged}/10 • Turnover: <strong>${t}</strong></div>`;
    // Plain-English descriptors
    q('#descriptorBlock').innerHTML = describeEra(parity, churn, year);
  } else {
    q('#churnBlock').innerHTML = '<div>Pick a year to compute churn.</div>';
    q('#descriptorBlock').innerHTML = '<small class="expl">Tip: select a specific year to generate analyst takeaways for the chosen era.</small>';
  }
}

async function main(){
  await loadDivisions();
  q('#loadDivBtn')?.addEventListener('click', () => loadParityAndChurn().catch(e => alert(String(e?.message || e))));
}

main().catch(e => alert(String(e?.message || e)));

// Optional: time-series graphs across years using ranking years list
async function loadTimeSeries(div){
  // Use ranking-years as a proxy list of available years
  const years = await fetchJSON('/analytics/ranking-years');
  const from = Number(q('#yearFrom')?.value || 0) || 0;
  const to = Number(q('#yearTo')?.value || 9999) || 9999;
  const ys = years.filter(y => y >= 2000 && y >= from && y <= to);
  const giniPts = [];
  const churnPts = [];
  for (const y of ys) {
    try {
      const p = await fetchJSON(`/analytics/division-parity?division=${encodeURIComponent(div)}&year=${y}`);
      giniPts.push({ y, v: p.gini });
      const c = await fetchJSON(`/analytics/division-churn?division=${encodeURIComponent(div)}&year=${y}`);
      churnPts.push({ y, v: c.avg_abs_delta });
    } catch {}
  }
  const pCanvas = q('#parityChart');
  const cCanvas = q('#churnChart');
  if (pCanvas && giniPts.length) {
    const ctx = pCanvas.getContext('2d');
    if (window.__parityChart) window.__parityChart.destroy();
    window.__parityChart = new Chart(ctx, {
      type: 'line',
      data: { labels: giniPts.map(p => String(p.y)), datasets: [{ label:'Gini', data: giniPts.map(p => p.v), borderColor:'#3ba3ff', tension:0.25, pointRadius:0 }] },
      options: { plugins:{ legend:{ display:false }, tooltip:{ callbacks:{ label:(ctx)=> `Gini: ${Number(ctx.parsed.y).toFixed(3)}` } } }, scales:{ x:{ display:true }, y:{ display:true } } },
    });
  }
  if (cCanvas && churnPts.length) {
    const ctx = cCanvas.getContext('2d');
    if (window.__churnChart) window.__churnChart.destroy();
    window.__churnChart = new Chart(ctx, {
      type: 'line',
      data: { labels: churnPts.map(p => String(p.y)), datasets: [{ label:'Avg |ΔELO|', data: churnPts.map(p => p.v), borderColor:'#10b981', tension:0.25, pointRadius:0 }] },
      options: { plugins:{ legend:{ display:false }, tooltip:{ callbacks:{ label:(ctx)=> `Avg |ΔELO|: ${Number(ctx.parsed.y).toFixed(2)}` } } }, scales:{ x:{ display:true }, y:{ display:true } } },
    });
  }
}

// Enhance load button to include time series
document.addEventListener('DOMContentLoaded', () => {
  const btn = q('#loadDivBtn');
  btn?.addEventListener('click', async () => {
    const div = q('#divSelect')?.value;
    try { await loadTimeSeries(div); } catch {}
  });
});

// Heuristic descriptors based on provided guidance
function describeEra(parity, churn, year){
  const g = Number(parity?.gini ?? 0);
  const n = Number(parity?.count ?? 0);
  const turnover = (churn && churn.top10_turnover != null) ? Number(churn.top10_turnover) : null;
  const avgAbs = (churn && churn.avg_abs_delta != null) ? Number(churn.avg_abs_delta) : null;

  // Thresholds (can be tuned):
  // Gini: low < 0.18, high > 0.26; Turnover: low < 0.20, high > 0.45
  const GINI_LOW = 0.18, GINI_HIGH = 0.26;
  const TURN_LOW = 0.20, TURN_HIGH = 0.45;

  let label = 'Mixed';
  if (g >= GINI_HIGH && (turnover !== null && turnover <= TURN_LOW)) label = 'Stratified era';
  else if (g <= GINI_LOW && (turnover !== null && turnover >= TURN_HIGH)) label = 'Deep, competitive era';

  const parts = [];
  parts.push(`<div><strong>${label}</strong> — ${year ?? ''} ${parity?.label ? '(' + parity.label + ')' : ''}</div>`);
  parts.push(`<div style="color:var(--muted)">A high Gini + low Churn year suggests a stratified era (dominant champ/static top tier). Low Gini + high Churn suggests a deep, competitive division with frequent ranking changes.</div>`);

  // Context cautions
  const cautions = [];
  if (n && n < 12) cautions.push('Small sample (few ranked fighters) can make Gini noisy.');
  if (avgAbs !== null && turnover !== null && avgAbs < 0.15 && turnover < TURN_LOW)
    cautions.push('Injuries/layoffs (fewer fights) can depress Churn even if true gaps are shifting.');
  if (cautions.length) parts.push(`<div style="color:var(--muted)"><em>Context:</em> ${cautions.join(' ')}</div>`);

  // Quick metrics recap
  const tStr = turnover !== null ? `${(turnover*100).toFixed(1)}%` : '—';
  parts.push(`<div style="margin-top:6px;">Gini: <strong>${fmt2(g)}</strong> • Turnover: <strong>${tStr}</strong> • Avg |ΔELO|: <strong>${fmt2(avgAbs)}</strong> • N: ${n || '—'}</div>`);
  return parts.join('');
}
