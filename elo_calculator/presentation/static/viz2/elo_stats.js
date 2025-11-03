// Bootstrap for ELO stats page: reuse helpers from app.js

window.addEventListener('DOMContentLoaded', () => {
  // Initialize counters for pagination
  window.__lbCounts = { highG: 10, lowG: 10, highL: 10 };

  // Wire buttons
  const moreHG = document.getElementById('btnMoreHighGains');
  const moreLG = document.getElementById('btnMoreLowGains');
  const moreHL = document.getElementById('btnMoreHighLosses');
  if (moreHG) moreHG.addEventListener('click', () => { window.__lbCounts.highG += 10; loadChangeLeaderboards(); });
  if (moreLG) moreLG.addEventListener('click', () => { window.__lbCounts.lowG += 10; loadChangeLeaderboards(); });
  if (moreHL) moreHL.addEventListener('click', () => { window.__lbCounts.highL += 10; loadChangeLeaderboards(); });

  // Header search View handler (provided by app.js)
  const viewBtn = document.getElementById('viewBtn');
  if (viewBtn) viewBtn.addEventListener('click', async () => {
    const q = (document.getElementById('qHeader')?.value || '').trim();
    if (!q) return;
    let fighterId = q;
    if (/\s/.test(q) || q.length < 8) {
      try {
        const results = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=5`);
        if (!results || results.length === 0) return alert('No fighters found');
        fighterId = results[0].fighter_id;
      } catch {
        return alert('Search failed');
      }
    }
    window.location = `/viz/fighter.html?id=${encodeURIComponent(fighterId)}`;
  });
  try { attachAutocomplete('qHeader'); } catch {}

  // Initial load
  if (typeof loadChangeLeaderboards === 'function') {
    loadChangeLeaderboards();
  }
});

