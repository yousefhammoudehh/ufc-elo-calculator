// Inject shared top nav from _nav.html and wire header search
(function initGlobalPageLoader(){
  if (window.__pageLoaderInit) return;
  window.__pageLoaderInit = true;
  const style = document.createElement('style');
  style.textContent = `
  #__pageLoader{position:fixed;inset:0;display:flex;align-items:center;justify-content:center;background:rgba(11,14,19,0.85);backdrop-filter:blur(2px);z-index:9999;opacity:0;pointer-events:none;transition:opacity .15s ease-in-out}
  #__pageLoader.show{opacity:1;pointer-events:auto}
  #__pageLoader .box{display:flex;flex-direction:column;align-items:center;gap:12px;color:#e5eef8}
  #__pageLoader .spinner{width:26px;height:26px;border:4px solid #1f2937;border-top-color:#3ba3ff;border-radius:50%;animation:__spin .9s linear infinite}
  @keyframes __spin { to { transform: rotate(360deg); } }
  `;
  document.head.appendChild(style);
  const overlay = document.createElement('div');
  overlay.id = '__pageLoader';
  overlay.innerHTML = `<div class="box"><div class="spinner"></div><div>Loading…</div></div>`;
  document.addEventListener('DOMContentLoaded', ()=>{ document.body.appendChild(overlay); });
  let pending = 0;
  const update = () => {
    const el = document.getElementById('__pageLoader');
    if (!el) return;
    if (pending > 0) el.classList.add('show'); else el.classList.remove('show');
  };
  const origFetch = window.fetch.bind(window);
  window.fetch = async function() {
    try { pending++; update(); } catch {}
    try { return await origFetch.apply(this, arguments); }
    finally { try { pending = Math.max(0, pending - 1); update(); } catch {} }
  };
  // Expose manual controls if a page wants to block around non-fetch async
  window.PageLoader = {
    block(){ pending++; update(); },
    unblock(){ pending = Math.max(0, pending - 1); update(); },
    pending(){ return pending; }
  };
})();

(async function injectNav() {
  try {
    if (window.__navInjected) return; // prevent double inject
    const host = document.getElementById('topNavHost') || document.body;
    const res = await fetch('./_nav.html', { cache: 'no-cache' });
    if (!res.ok) return;
    const html = await res.text();
    const wrapper = document.createElement('div');
    wrapper.innerHTML = html;
    const node = wrapper.firstElementChild;
    if (node) {
      if (host === document.body) {
        document.body.insertBefore(node, document.body.firstChild);
      } else {
        host.replaceWith(node);
      }
    }
    window.__navInjected = true;
    // Wire header search
    const viewBtn = document.getElementById('viewBtn');
    if (viewBtn) viewBtn.addEventListener('click', async () => {
      const qEl = document.getElementById('qHeader');
      const q = (qEl && 'value' in qEl ? qEl.value : '').trim();
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
  } catch {}
})();
