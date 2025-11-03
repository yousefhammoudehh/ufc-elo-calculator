/* global document, fetch, alert */

async function fetchJSON(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data && data.message) || `Request failed ${res.status}`);
  return data.data ?? data;
}
function q(sel){ return document.querySelector(sel); }
function fmt2(x){ if (x === null || x === undefined) return '—'; return Number(x).toFixed(2); }

async function loadEventsList(){
  try {
    const items = await fetchJSON('/analytics/events');
    const sel = q('#eventSelect');
    sel.innerHTML = items.map(it => `<option value="${it.event_id}">${it.name || it.event_date || it.event_id}</option>`).join('');
    const u = new URL(window.location.href);
    const eid = u.searchParams.get('event_id');
    if (eid) sel.value = eid;
  } catch (e) { console.warn('Failed to load events', e); }
}

async function loadEventShock(eventId){
  const shock = eventId ? await fetchJSON(`/analytics/event-shock?event_id=${encodeURIComponent(eventId)}`) : await fetchJSON('/analytics/latest-event-shock');
  {
    const host = q('#eventTitle');
    const label = shock.event_name || 'Event';
    const id = eventId || shock.event_id;
    if (host) host.innerHTML = id ? `<a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(id)}">${label}</a>` : label;
  }
  q('#eventMeta').textContent = shock.event_name ? '' : (shock.event_date || '');
  q('#shockBlock').innerHTML = `<div>Shock Index: <strong>${fmt2(shock.shock)}</strong> • Net ELO Transfer: <strong>${fmt2(shock.net_transfer)}</strong></div>`;
  // Analysis text using empirical thresholds provided
  const tierShock = (s) => (s < 5.5 ? 'Predictable' : s <= 7.5 ? 'Balanced' : s <= 9.5 ? 'Upset-Heavy' : 'Chaotic');
  const tierAvg = (a) => (a < 0.6 ? 'Predictable' : a <= 0.8 ? 'Balanced' : a <= 1.0 ? 'Upset-Heavy' : 'Chaotic');
  const categorizeTransfer = (t) => (t <= 150 ? 'low movement' : t <= 300 ? 'mid-high movement' : 'very high movement');
  const fightsCount = Number(shock.fights || (shock.count || 0));
  const S = Number(shock.shock || 0);
  const T = Number(shock.net_transfer || 0);
  const avgShock = fightsCount > 0 ? (S / fightsCount) : null;
  const sTier = tierShock(S);
  const aTier = avgShock != null ? tierAvg(avgShock) : null;
  const tCat = categorizeTransfer(T);
  let quadrant = '';
  if (sTier === 'Predictable' && tCat === 'low movement') quadrant = 'Predictable card, stable rankings';
  else if ((sTier === 'Upset-Heavy' || sTier === 'Chaotic') && tCat === 'low movement') quadrant = 'Many small upsets (surprises but modest ranking impact)';
  else if (sTier === 'Predictable' && (tCat === 'mid-high movement' || tCat === 'very high movement')) quadrant = 'Expected results but high stakes (large ELO moves)';
  else quadrant = 'Chaos night: major upsets with big ranking shifts';
  const analysis = [
    `<div><strong>Reading:</strong> ${sTier} (shock) • ${tCat} (transfer) — ${quadrant}.</div>`,
    avgShock != null ? `<div>Avg surprise per fight: <strong>${avgShock.toFixed(2)}</strong> (${aTier})</div>` : '',
  ].filter(Boolean).join('');
  q('#analysisBlock').innerHTML = analysis;
  // Also load bout entries using existing endpoint for details
  const evData = eventId ? await fetchJSON(`/analytics/event-elo?event_id=${encodeURIComponent(eventId)}`) : await fetchJSON('/analytics/latest-event-elo');
  const tbody = q('#tblBoutSwings tbody');
  const fmtDelta = (x) => { const n = Number(x); if (!Number.isFinite(n)) return '-'; return n >= 0 ? `+${n.toFixed(2)}` : n.toFixed(2); };
  const entries = Array.from(evData.entries || []);
  entries.sort((a, b) => {
    const at = a.is_title_fight ? 1 : 0;
    const bt = b.is_title_fight ? 1 : 0;
    if (bt - at !== 0) return bt - at; // titles first
    const amag = Math.max(Math.abs(Number(a.fighter1_delta||0)), Math.abs(Number(a.fighter2_delta||0)));
    const bmag = Math.max(Math.abs(Number(b.fighter1_delta||0)), Math.abs(Number(b.fighter2_delta||0)));
    return bmag - amag; // then by max absolute swing
  });
  const rows = entries.map((r) => {
    const f1 = r.fighter1_id ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter1_id)}">${r.fighter1_name || r.fighter1_id}</a>` : (r.fighter1_name || '-');
    const f2 = r.fighter2_id ? `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(r.fighter2_id)}">${r.fighter2_name || r.fighter2_id}</a>` : (r.fighter2_name || '-');
    const bout = r.bout_id ? `<a class="link-muted" href="/viz/bout.html?bout_id=${encodeURIComponent(r.bout_id)}">Open</a>` : '-';
    const tf = r.is_title_fight ? ' <span class="badge title">Title</span>' : '';
    const d1 = Number(r.fighter1_delta); const d2 = Number(r.fighter2_delta);
    const d1c = d1>=0? 'delta pos':'delta neg';
    const d2c = d2>=0? 'delta pos':'delta neg';
    return `<tr>
      <td>${f1}${tf} vs ${f2}</td>
      <td>${r.fighter1_outcome || '-'} / ${r.fighter2_outcome || '-'}</td>
      <td><span class="${d1c}">${fmtDelta(d1)}</span> / <span class="${d2c}">${fmtDelta(d2)}</span></td>
      <td>${bout}</td>
    </tr>`;
  }).join('');
  tbody.innerHTML = rows || '<tr><td colspan="4" style="color:var(--muted)">No data</td></tr>';
}

async function main(){
  await loadEventsList();
  // Load top/bottom shocking lists
  try {
    const most = await fetchJSON('/analytics/events-shock-top?limit=5&order=desc&max_events=300');
    const least = await fetchJSON('/analytics/events-shock-top?limit=5&order=asc&max_events=300');
    const renderTbl = (id, arr) => {
      const tb = q(`#${id} tbody`);
      if (!tb) return;
      tb.innerHTML = (arr||[]).map(r => {
        const label = r.event_name || r.event_date || r.event_id;
        const eid = r.event_id;
        const link = eid ? `<a class="link-muted" href="/viz/event_insights.html?event_id=${encodeURIComponent(eid)}">${label}</a>` : label;
        return `<tr><td>${link}</td><td>${fmt2(r.shock)}</td><td>${fmt2(r.net_transfer)}</td></tr>`;
      }).join('');
    };
    renderTbl('tblMostShocking', most);
    renderTbl('tblMostPredictable', least);
  } catch {}
  q('#loadEventBtn')?.addEventListener('click', async () => {
    const sel = q('#eventSelect');
    const id = sel?.value;
    await loadEventShock(id);
  });
  q('#loadLatestBtn')?.addEventListener('click', async () => {
    await loadEventShock(null);
  });
  // Load preselected event if provided, otherwise currently selected in dropdown, else latest
  try {
    const u0 = new URL(window.location.href);
    const eid0 = u0.searchParams.get('event_id');
    const sel0 = q('#eventSelect');
    const chosen = eid0 || (sel0 && sel0.value) || null;
    await loadEventShock(chosen);
  } catch {
    await loadEventShock(null);
  }
}

main().catch(e => alert(String(e?.message || e)));
