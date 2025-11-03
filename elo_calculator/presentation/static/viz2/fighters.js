async function fetchJSON(url) {
  const doFetch = async (u) => {
    const res = await fetch(u);
    return res;
  };
  let res = await doFetch(url);
  // Fallback: if 404 and not already prefixed, try /api prefix (useful behind reverse proxies)
  if (res.status === 404 && typeof url === 'string' && url.startsWith('/') && !url.startsWith('/api/')) {
    res = await doFetch(`/api${url}`);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  if (!payload) throw new Error('Empty response');
  if (typeof payload.status_code === 'number' && payload.status_code !== 200) {
    throw new Error(payload.message || `HTTP ${payload.status_code}`);
  }
  return payload.data !== undefined ? payload.data : payload;
}

let state = { q: '', page: 1, limit: 20, sort_by: 'name', order: 'desc', total: 0 };

function renderResults(list) {
  const el = document.getElementById('results');
  el.innerHTML = '';
  if (!list || list.length === 0) {
    el.innerHTML = '<small>No fighters found</small>';
    return;
  }
  for (const f of list) {
    const item = document.createElement('div');
    item.className = 'item';
    const left = document.createElement('div');
    left.innerHTML = `<div style="font-weight:600;">${f.name}</div><small>ID: ${f.fighter_id}</small>`;
    const right = document.createElement('div');
    const curr = typeof f.current_elo === 'number' ? f.current_elo.toFixed(2) : '—';
    const peak = typeof f.peak_elo === 'number' ? f.peak_elo.toFixed(2) : '—';
    right.innerHTML = `<small>Current ELO: ${curr} | Peak: ${peak}</small> `+
      `<a class="link" href="/viz/fighter.html?id=${encodeURIComponent(f.fighter_id)}">View →</a>`;
    item.appendChild(left);
    item.appendChild(right);
    el.appendChild(item);
  }
}

function renderPagination() {
  const pageInfo = document.getElementById('pageInfo');
  const totalPages = Math.max(1, Math.ceil(state.total / state.limit));
  pageInfo.textContent = `Page ${state.page} of ${totalPages} — ${state.total} fighters`;
  document.getElementById('prevBtn').disabled = state.page <= 1;
  document.getElementById('nextBtn').disabled = state.page >= totalPages;
}

async function doSearch() {
  state.q = document.getElementById('q').value.trim();
  state.limit = parseInt(document.getElementById('pageSize').value, 10);
  state.sort_by = document.getElementById('sortBy').value;
  state.order = document.getElementById('order').value;
  try {
    const url = `/fighters/search-paginated?q=${encodeURIComponent(state.q)}&page=${state.page}&limit=${state.limit}&sort_by=${state.sort_by}&order=${state.order}`;
    const payload = await fetchJSON(url);
    renderResults(payload.items);
    state.total = payload.total || 0;
    renderPagination();
  } catch (e) {
    console.error('Search failed', e);
    renderResults([]);
    state.total = 0;
    renderPagination();
  }
}

window.addEventListener('DOMContentLoaded', () => {
  document.getElementById('searchBtn').addEventListener('click', doSearch);
  document.getElementById('prevBtn').addEventListener('click', () => { if (state.page > 1) { state.page--; doSearch(); } });
  document.getElementById('nextBtn').addEventListener('click', () => { state.page++; doSearch(); });
  document.getElementById('sortBy').addEventListener('change', () => { state.page = 1; doSearch(); });
  document.getElementById('order').addEventListener('change', () => { state.page = 1; doSearch(); });
  document.getElementById('pageSize').addEventListener('change', () => { state.page = 1; doSearch(); });
  // Initial load
  doSearch();
});
