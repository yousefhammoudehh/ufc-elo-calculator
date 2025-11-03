async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  return payload.data !== undefined ? payload.data : payload;
}

const METRICS = [
  ['kd', 'Knockdowns'],
  ['td', 'Takedowns'],
  ['td_attempts', 'TD Attempts'],
  ['control_time_sec', 'Control Time (s)'],
  ['sub_attempts', 'Submission Attempts'],
  ['reversals', 'Reversals'],
  ['sig_strikes', 'Sig. Strikes'],
  ['sig_strikes_thrown', 'Sig. Strikes Attempted'],
  ['total_strikes', 'Total Strikes'],
  ['total_strikes_thrown', 'Total Strikes Attempted'],
  ['head_ss', 'Head Sig. Strikes'],
  ['body_ss', 'Body Sig. Strikes'],
  ['leg_ss', 'Leg Sig. Strikes'],
  ['distance_ss', 'Distance Sig. Strikes'],
  ['clinch_ss', 'Clinch Sig. Strikes'],
  ['ground_ss', 'Ground Sig. Strikes'],
];

// Metric grouping removed; show full list in a single dropdown

let CURRENT_LIST = METRICS;
let CURRENT_IDX = 0;

function populateMetricSelect() {
  const sel = document.getElementById('tsMetric');
  if (!sel) return;
  const currentKey = CURRENT_LIST[CURRENT_IDX]?.[0];
  sel.innerHTML = CURRENT_LIST.map(([k, t]) => `<option value="${k}">${t}</option>`).join('');
  // restore selection by key if possible
  const idx = CURRENT_LIST.findIndex(([k]) => k === currentKey);
  sel.value = idx >= 0 ? CURRENT_LIST[idx][0] : CURRENT_LIST[0][0];
}

async function populateDivisions() {
  const sel = document.getElementById('tsDivisionSel');
  if (!sel) return;
  try {
    const items = await fetchJSON('/analytics/divisions');
    sel.innerHTML = items.map(it => `<option value="${it.code}">${it.label}</option>`).join('');
  } catch {
    sel.innerHTML = '<option value="">All divisions</option>';
  }
}

async function renderSingleCard(params){
  const host = document.getElementById('statsHost');
  host.innerHTML = '';
  const q = new URLSearchParams(params).toString();
  const [key, title] = CURRENT_LIST[CURRENT_IDX];
  const items = await fetchJSON(`/analytics/top-stats?metric=${encodeURIComponent(key)}&limit=10&${q}`);
  const card = document.createElement('div');
  card.className = 'card';
  card.innerHTML = `
    <h2>${title}</h2>
    <table>
      <thead><tr><th>Fighter</th><th class="val">${title}</th><th class="val">Fights</th></tr></thead>
      <tbody>${(items||[]).map((x) => {
        const name = x.name ? `<a class=\"link-muted\" href=\"/viz/fighter.html?id=${encodeURIComponent(x.fighter_id)}\">${x.name}</a>` : (x.fighter_id || '-');
        const isDiff = params && params.adjusted === 'true';
        const vnum = Number(x.value ?? 0);
        const vdisp = isDiff ? vnum.toFixed(2) : (Number.isInteger(vnum) ? vnum : vnum.toFixed(2));
        return `<tr><td>${name}</td><td class=\"val\">${vdisp}</td><td class=\"val\">${Number(x.fights ?? 0)}</td></tr>`;
      }).join('')}</tbody>
    </table>`;
  host.appendChild(card);
}

window.addEventListener('DOMContentLoaded', async () => {
  const apply = async () => {
    const since = document.getElementById('tsSince').value;
    const divSel = document.getElementById('tsDivisionSel');
    const div = divSel ? divSel.value : '';
    const rate = document.getElementById('tsRate').value;
    const adj = document.getElementById('tsAdjusted').checked;
    const params = {};
    if (since) params.since_year = since;
    if (div) params.division = div;
    if (rate) params.rate = rate;
    if (adj) params.adjusted = 'true';
    await renderSingleCard(params);
  };
  CURRENT_LIST = METRICS;
  CURRENT_IDX = 0;
  // initial metric list for current group
  await populateDivisions();
  // Ensure an 'All divisions' option is present at the top
  (function ensureAllDiv(){
    const sel = document.getElementById('tsDivisionSel');
    if (sel && !sel.querySelector('option[value=""]')) {
      sel.insertAdjacentHTML('afterbegin', '<option value="">All divisions</option>');
    }
  })();
  populateMetricSelect();
  await apply();
  document.getElementById('tsApply')?.addEventListener('click', apply);
  // No metric-group filter; single metric dropdown controls selection
  document.getElementById('tsMetric')?.addEventListener('change', async (e)=>{
    const key = e.target.value;
    const idx = CURRENT_LIST.findIndex(([k]) => k === key);
    if (idx >= 0) CURRENT_IDX = idx;
    await apply();
  });
});
