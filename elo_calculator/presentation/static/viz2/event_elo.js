async function fetchEventElo(eventId) {
  // Prefer a general endpoint if present; fallback to latest
  if (eventId) {
    try {
      // This will work once /analytics/event-elo exists
      return await fetchJSON(`/analytics/event-elo?event_id=${encodeURIComponent(eventId)}`);
    } catch (e) {
      console.warn('event-elo endpoint not available or failed; falling back to latest', e);
    }
  }
  return await fetchJSON('/analytics/latest-event-elo');
}

async function loadEvent(eventId) {
  const loader = document.getElementById('loader');
  if (loader) loader.style.display = 'flex';
  try {
    const data = await fetchEventElo(eventId);
    const tbody = document.getElementById('tbl');
    if (tbody) {
      const entries = Array.isArray(data?.entries) ? data.entries : [];
      tbody.innerHTML = entries.map((r) => {
        const f1n = r.fighter1_name || r.fighter1_id || '-';
        const f2n = r.fighter2_name || r.fighter2_id || '-';
        const boutText = `${f1n} vs ${f2n}`;
        const outText = `${r.fighter1_outcome || '-'} / ${r.fighter2_outcome || '-'}`;
        const d1 = Number.isFinite(Number(r.fighter1_delta)) ? Number(r.fighter1_delta) : null;
        const d2 = Number.isFinite(Number(r.fighter2_delta)) ? Number(r.fighter2_delta) : null;
        const deltaCell = `${d1 != null ? `<span class="delta ${d1 >= 0 ? 'pos' : 'neg'}">${fmtDelta(d1)}</span>` : '-'} / ${d2 != null ? `<span class="delta ${d2 >= 0 ? 'pos' : 'neg'}">${fmtDelta(d2)}</span>` : '-'}`;
        const open = r.bout_id ? `<a class="link-muted" href="/viz/bout.html?bout_id=${encodeURIComponent(r.bout_id)}">Open</a>` : '-';
        return `<tr><td>${boutText}</td><td>${outText}</td><td>${deltaCell}</td><td>${open}</td></tr>`;
      }).join('');
    }
  } catch (e) {
    console.error('Failed to load event elo', e);
  } finally {
    if (loader) loader.style.display = 'none';
  }
}

function getQueryParam(name) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name);
}

window.addEventListener('DOMContentLoaded', () => {
  const initialById = getQueryParam('event_id');
  const initialByDate = getQueryParam('date');
  initEventNavigator(initialById, initialByDate);
});

let __events = [];
let __curIdx = -1;

async function initEventNavigator(initialEventId, initialDate) {
  try {
    __events = await fetchJSON('/analytics/events');
    __events.sort((a,b) => String(a.event_date||'') < String(b.event_date||'') ? -1 : 1);
    if (!__events.length) return;
    if (initialEventId) {
      const idx = __events.findIndex((e) => String(e.event_id) === String(initialEventId));
      __curIdx = idx >= 0 ? idx : __events.length - 1;
    } else if (initialDate) {
      const idx = __events.findIndex((e) => String(e.event_date) === String(initialDate));
      __curIdx = idx >= 0 ? idx : __events.length - 1;
    } else {
      __curIdx = __events.length - 1; // latest by default
    }
    await selectEventByIndex(__curIdx);
    const prev = document.getElementById('btnPrevEvent');
    const next = document.getElementById('btnNextEvent');
    if (prev) prev.addEventListener('click', async () => { if (__curIdx > 0) { __curIdx -= 1; await selectEventByIndex(__curIdx); } });
    if (next) next.addEventListener('click', async () => { if (__curIdx < __events.length - 1) { __curIdx += 1; await selectEventByIndex(__curIdx); } });
    // Populate date selector
    const sel = document.getElementById('eventDateSelect');
    if (sel) {
      sel.innerHTML = __events.map((e, i) => `<option value="${i}">${e.name || e.event_date || 'Event'}</option>`).join('');
      sel.value = String(__curIdx);
      sel.addEventListener('change', async () => {
        __curIdx = Number(sel.value);
        await selectEventByIndex(__curIdx);
      });
    }
  } catch (e) {
    console.error('Failed to init event navigator', e);
  }
}

async function selectEventByIndex(idx) {
  if (!__events.length || idx < 0 || idx >= __events.length) return;
  const ev = __events[idx];
  const label = document.getElementById('eventLabel');
  if (label) label.textContent = `${ev.name || 'Event'}`;
  await loadEvent(ev.event_id);
}
