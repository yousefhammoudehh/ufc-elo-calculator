async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  return payload.data !== undefined ? payload.data : payload;
}

function fmtDelta(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return '-';
  return n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2);
}

window.addEventListener('DOMContentLoaded', async () => {
  const meta = document.getElementById('eventMeta');
  const tbody = document.getElementById('tbl');
  const loader = document.getElementById('loader');
  try {
    const data = await fetchJSON('/analytics/latest-event-elo');
    if (meta) meta.textContent = `${data.event_name || ''}`;
    const rows = (data.entries || []).map((r) => {
      const f1 = r.fighter1_name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter1_id)}">${r.fighter1_name}</a>` : (r.fighter1_id || '-');
      const f2 = r.fighter2_name ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter2_id)}">${r.fighter2_name}</a>` : (r.fighter2_id || '-');
      const bout = r.bout_id ? `<a class="link-muted" href="/viz/bout.html?bout_id=${encodeURIComponent(r.bout_id)}">Open</a>` : '-';
      const tf = r.is_title_fight ? ' <span class="badge title">Title</span>' : '';
      const d1 = Number(r.fighter1_delta); const d2 = Number(r.fighter2_delta);
      return `<tr>
        <td>${f1}${tf} vs ${f2}</td>
        <td>${r.fighter1_outcome || '-'} / ${r.fighter2_outcome || '-'}</td>
        <td><span class="${d1>=0?'delta pos':'delta neg'}">${fmtDelta(d1)}</span> / <span class="${d2>=0?'delta pos':'delta neg'}">${fmtDelta(d2)}</span></td>
        <td>${bout}</td>
      </tr>`;
    }).join('');
    tbody.innerHTML = rows || '<tr><td colspan="4" style="color:var(--muted)">No data</td></tr>';
  } catch (e) {
    console.error('Failed to load latest event', e);
    if (tbody) tbody.innerHTML = '<tr><td colspan="7" style="color:var(--muted)">Failed to load</td></tr>';
  } finally {
    if (loader) loader.remove();
  }
});
