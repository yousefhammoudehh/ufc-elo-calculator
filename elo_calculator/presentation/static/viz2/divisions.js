async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  return payload.data !== undefined ? payload.data : payload;
}

function setValHead(metric) {
  const head = document.getElementById('valHead');
  if (!head) return;
  const year = document.getElementById('yearSel')?.value || '';
  if (metric === 'gains') {
    head.textContent = year ? 'Δ ELO (year)' : 'Δ ELO (all-time)';
  } else if (metric === 'current') {
    head.textContent = 'Current ELO';
  } else {
    head.textContent = 'Peak ELO';
  }
}

function renderRows(items) {
  const tbody = document.getElementById('tblBody');
  if (!tbody) return;
  tbody.innerHTML = (items || []).slice(0, 50).map((x, i) => {
    const name = x.name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(x.fighter_id)}">${x.name}</a>` : (x.fighter_id || '-');
    const val = Number.isFinite(Number(x.value)) ? Number(x.value).toFixed(2) : String(x.value ?? '-');
    return `<tr><td>${i + 1}</td><td>${name}</td><td class="val">${val}</td></tr>`;
  }).join('');
}

async function loadDivisions() {
  const sel = document.getElementById('divisionSel');
  const list = await fetchJSON('/analytics/divisions');
  if (sel) sel.innerHTML = (list || []).map((d) => `<option value="${d.code}">${d.label}</option>`).join('');
}

async function loadYears() {
  const years = await fetchJSON('/analytics/ranking-years');
  const sel = document.getElementById('yearSel');
  if (sel) sel.innerHTML = ['','...'].includes(years) ? '' : [''].concat(years).map((y) => y ? `<option value="${y}">${y}</option>` : `<option value="">Any</option>`).join('');
}

async function apply() {
  const division = document.getElementById('divisionSel')?.value;
  const metric = document.getElementById('metricSel')?.value || 'current';
  const yearStr = document.getElementById('yearSel')?.value || '';
  const activeOnly = !!document.getElementById('activeOnly')?.checked;
  setValHead(metric);
  const params = new URLSearchParams();
  params.set('division', division);
  params.set('metric', metric);
  if (yearStr) params.set('year', yearStr);
  if (activeOnly) params.set('active_only', 'true');
  const items = await fetchJSON(`/analytics/division-rankings?${params.toString()}`);
  renderRows(items);
}

window.addEventListener('DOMContentLoaded', async () => {
  await Promise.all([loadDivisions(), loadYears()]);
  document.getElementById('applyBtn')?.addEventListener('click', apply);
  document.getElementById('metricSel')?.addEventListener('change', () => setValHead(document.getElementById('metricSel').value));
  await apply();
});
