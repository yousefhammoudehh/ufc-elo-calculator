import { api } from '../api.js';
import { SkeletonCard, SkeletonTable } from './components/skeleton.js';
import { ErrorCard } from './components/error.js';

export async function mountHome() {
  // Latest Event (meta + top bout upsets for that event)
  const latest = document.getElementById('card-latest');
  latest.innerHTML = SkeletonCard(5);
  try {
    // meta + global latest shock + latest event shock detail (includes top_bouts)
    const [elo, eventShockDetail] = await Promise.all([
      api.latestEventElo(),
      api.eventShock() // omitting event_id returns latest event shock + top_bouts
    ]);
    const sIdx = (elo?.shock_index ?? eventShockDetail?.shock_index ?? null);

    latest.innerHTML = LatestEventCard({
      name: elo?.event_name ?? 'Latest Event',
      date: elo?.event_date ?? '—',
      shock_index: sIdx,
      title_bouts: elo?.title_bouts ?? null,
      entries: elo?.entries ?? []
    });
  } catch (e) {
    latest.innerHTML = ErrorCard(e);
  }


  // In-Form Fighters (last 365d)
  const inform = document.getElementById('card-inform');
  inform.innerHTML = SkeletonTable(8, 3);
  try {
    const data = await api.formTop({ window: 'fights', n: 5, top: 10, min_recent_fights: 2, recent_days: 365 });
    inform.innerHTML = InFormList(data || []);
  } catch (e) {
    inform.innerHTML = ErrorCard(e);
  }

  // Highlights (top3 current, most shocking event, top3 peak)
  const hl = document.getElementById('card-highlights');
  hl.innerHTML = `<div class="card">${SkeletonCard(2)}</div>`;
  try {
    const [top3, shockingArr, peak3] = await Promise.all([
      api.topElo(3),  // Highest current ELO (top 3)
      api.eventsShockTop({ type: 'shocking', range: '90d', limit: 1 }), // Most shocking event
      api.topPeakElo(3) // Top peak ELO (top 3)
    ]);

    const shocking = Array.isArray(shockingArr) ? shockingArr[0] : null;

    hl.innerHTML = [
      HighlightCard('Highest current ELO', FightersList(top3, 'current')),
      HighlightCard('Most shocking event (90d)', formatEvent(shocking, true)),
      HighlightCard('Top peak ELO', FightersList(peak3, 'peak'))
    ].join('');
  } catch (e) {
    hl.innerHTML = ErrorCard(e);
  }



  // Rankings (Current / Peak)
  const rk = document.getElementById('card-rankings');
  rk.innerHTML = RankingsSkeleton();
  let mode = 'current';
  async function renderRankings() {
    rk.innerHTML = RankingsShell(mode);
    const tbody = rk.querySelector('tbody');
    tbody.innerHTML = `<tr><td colspan="3" class="py-3">Loading…</td></tr>`;
    try {
      const rows = mode === 'current' ? await api.topElo(10) : await api.topPeakElo(10);
      const max = Math.max(...rows.map(r => r.current_elo ?? r.peak_elo ?? 0), 1);
      tbody.innerHTML = rows.map((r, i) => RankingRow({
        rank: i + 1,
        name: r.name,
        value: Math.round((mode === 'current' ? r.current_elo : r.peak_elo) ?? 0),
        pct: Math.round(100 * ((mode === 'current' ? r.current_elo : r.peak_elo) ?? 0) / max)
      })).join('');
    } catch (e) {
      tbody.innerHTML = `<tr><td colspan="3">${ErrorCard(e)}</td></tr>`;
    }
  }
  rk.addEventListener('click', (e) => {
    const t = e.target.closest('[data-mode]');
    if (!t) return;
    mode = t.dataset.mode; renderRankings();
  });
  renderRankings();

  // Recent Shocks (top 10) + show shock value
  const shocks = document.getElementById('card-shocks');
  shocks.innerHTML = SkeletonTable(10, 3);
  try {
    const data = await api.eventsShockTop({ type: 'shocking', range: '90d', limit: 10 });
    shocks.innerHTML = RecentShocksList(data || []);
  } catch (e) {
    shocks.innerHTML = ErrorCard(e);
  }

}

/*************** Components ***************/
function InFormList(items) {
  return `
    <div class="space-y-3">
      <div class="card-title">In-Form Fighters (last 12 months)</div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead><tr class="text-slate-500">
            <th class="py-2 text-left">Fighter</th>
            <th class="py-2 text-right">FI</th>
            <th class="py-2 text-right">Recent</th>
          </tr></thead>
          <tbody>
            ${(items || []).slice(0, 10).map(r => `
              <tr class="border-b border-slate-100">
                <td class="py-2 pr-2">${escape(r.fighter_name)}</td>
                <td class="py-2 text-right">${fmt(r.fi)}</td>
                <td class="py-2 text-right">${r.recent_fights ?? '—'}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>
      <p class="text-xs text-slate-500">FI = Form Index over last 5 fights; higher means better recent performance vs opponent quality.</p>
    </div>`;
}

function HighlightCard(title, bodyHtml) {
  return `
    <div class="card">
      <div class="text-sm font-medium mb-1">${escape(title)}</div>
      <div class="text-sm">${bodyHtml || '<span class=\'text-slate-500\'>—</span>'}</div>
    </div>`;
}

function formatFighter(row) {
  if (!row) return '';
  const val = row.current_elo ?? row.peak_elo ?? null;
  return `${escape(row.name)} <span class="text-slate-500">•</span> <span class="font-semibold">${val ? Math.round(val) : '—'}</span>`;
}


function RankingsSkeleton() {
  return `
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <div class="card-title">Current Rankings</div>
        <div class="inline-flex text-xs rounded-lg border overflow-hidden">
          <button class="px-2 py-1 bg-slate-900 text-white" disabled>Current</button>
          <button class="px-2 py-1">Peak</button>
        </div>
      </div>
      ${SkeletonTable(8, 3)}
    </div>`;
}

function RankingsShell(mode) {
  return `
    <div class="space-y-3">
      <div class="flex items-center justify-between">
        <div class="card-title">${mode === 'current' ? 'Current Rankings' : 'Peak Rankings'}</div>
        <div class="inline-flex text-xs rounded-lg border overflow-hidden">
          <button data-mode="current" class="px-2 py-1 ${mode === 'current' ? 'bg-slate-900 text-white' : 'hover:bg-slate-100'}">Current</button>
          <button data-mode="peak" class="px-2 py-1 ${mode === 'peak' ? 'bg-slate-900 text-white' : 'hover:bg-slate-100'}">Peak</button>
        </div>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-sm">
          <thead><tr class="text-slate-500">
            <th class="py-2 text-left">#</th>
            <th class="py-2 text-left">Fighter</th>
            <th class="py-2 text-right">ELO</th>
          </tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <p class="text-xs text-slate-500">ELO rises more for wins over stronger opponents, and falls more for upsets.</p>
    </div>`;
}

function RankingRow({ rank, name, value, pct }) {
  return `
    <tr class="border-b border-slate-100">
      <td class="py-2 pr-3">${rank}</td>
      <td class="py-2 pr-3">
        <div class="flex items-center gap-2">
          <div class="w-40 lg:w-64"><div class="inline-bar"><i style="width:${pct}%"></i></div></div>
          <div class="truncate">${escape(name)}</div>
        </div>
      </td>
      <td class="py-2 text-right font-semibold">${value}</td>
    </tr>`;
}


function FightersList(rows = [], mode = 'current') {
  if (!Array.isArray(rows) || rows.length === 0) return '<span class="text-slate-500">—</span>';
  return `
    <ol class="space-y-1">
      ${rows.map((r, i) => {
    const name = escape(r.name ?? r.fighter_name ?? `#${i + 1}`);
    const val = mode === 'peak'
      ? (r.peak_elo ?? r.current_elo ?? null)
      : (r.current_elo ?? r.peak_elo ?? null);
    return `<li class="flex items-center justify-between">
          <span class="truncate">${i + 1}. ${name}</span>
          <span class="font-semibold">${val != null ? Math.round(val) : '—'}</span>
        </li>`;
  }).join('')}
    </ol>`;
}

function formatEvent(row, showShock = false) {
  if (!row || typeof row !== 'object') return '';
  const title = row.event_name ?? row.name ?? '';
  const shockRaw = row.shock_index ?? row.shock ?? null;
  const shockNum = shockRaw == null ? null : Number(shockRaw);
  const titleHtml = title ? escape(title) : '';
  if (!showShock) return titleHtml;
  const shockHtml = (shockNum != null && !Number.isNaN(shockNum))
    ? `<span class="badge">Shock <span class="font-semibold">${shockNum.toFixed(2)}</span></span>`
    : '';
  return `${titleHtml} ${shockHtml}`;
}

function RecentShocksList(items) {
  return `
    <div class="space-y-3">
      <div class="card-title">Recent Shocking Events (90d)</div>
      <div class="divide-y divide-slate-100">
        ${(items || []).map(row => {
    const name = escape(row.event_name ?? row.name ?? 'Event');
    const s = row.shock != null ? Number(row.shock) : null;
    const shock = (s != null && !Number.isNaN(s)) ? s.toFixed(2) : '—';
    return `
            <div class="py-2 flex items-center justify-between">
              <div class="truncate pr-3">${name}</div>
              <div class="text-sm"><span class="badge">Shock <span class="font-semibold">${shock}</span></span></div>
            </div>`;
  }).join('')}
      </div>
    </div>`;
}

// Expanded latest event card with top bout upsets
function LatestEventCard({ name, date, shock_index, title_bouts, entries = [] }) {
  const shockBadge = shock_index != null
    ? `<span class="badge">Shock Index <span class="font-semibold">${Number(shock_index).toFixed(2)}</span></span>`
    : '';
  const titles = (title_bouts != null)
    ? `<span class="badge">Title bouts <span class="font-semibold">${title_bouts}</span></span>`
    : '';

  const normalized = Array.isArray(entries)
    ? entries.filter((entry) => Number.isFinite(Number(entry?.fighter1_delta)) || Number.isFinite(Number(entry?.fighter2_delta)))
    : [];

  const titleFights = normalized.filter((entry) => entry?.is_title_fight);
  const nonTitle = normalized
    .filter((entry) => !entry?.is_title_fight)
    .sort((a, b) => boutMagnitude(b) - boutMagnitude(a));

  const ordered = [...titleFights, ...nonTitle].slice(0, 7);

  const boutsHtml = ordered.length
    ? `
      <div class="mt-3">
        <div class="text-sm font-medium mb-1">Title bouts & biggest ELO swings</div>
        <ol class="space-y-1 text-sm">
          ${ordered.map(renderLatestEntry).join('')}
        </ol>
      </div>`
    : '<p class="text-sm text-slate-500 mt-2">No bouts with ELO changes recorded for this event.</p>';

  return `
    <div class="space-y-3">
      <div class="card-title">Latest Event</div>
      <div>
        <div class="text-base font-medium">${escape(name)}</div>
        <div class="text-sm text-slate-600">${escape(date)}</div>
      </div>
      <div class="flex flex-wrap gap-2">${shockBadge}${titles}</div>
      <p class="text-sm text-slate-600">Titles are shown first, followed by the biggest ELO swings on the card.</p>
      ${boutsHtml}
    </div>`;
}


function renderLatestEntry(entry) {
  const fighters = [
    {
      name: escape(entry.fighter1_name ?? entry.fighter1_id ?? 'Fighter'),
      delta: Number(entry.fighter1_delta ?? 0)
    },
    {
      name: escape(entry.fighter2_name ?? entry.fighter2_id ?? 'Opponent'),
      delta: Number(entry.fighter2_delta ?? 0)
    }
  ].sort((a, b) => b.delta - a.delta);
  const [left, right] = fighters;
  const titleBadge = entry.is_title_fight ? ' <span class="badge">Title</span>' : '';
  return `<li class="flex items-center justify-between">
      <span class="truncate">(${formatDelta(left.delta)}) ${left.name} vs ${right.name} (${formatDelta(right.delta)})${titleBadge}</span>
    </li>`;
}

function boutMagnitude(entry) {
  const a = Math.abs(Number(entry.fighter1_delta ?? 0));
  const b = Math.abs(Number(entry.fighter2_delta ?? 0));
  return Math.max(a, b);
}

function formatDelta(val) {
  const num = Number(val);
  if (!Number.isFinite(num)) return '—';
  return `${num >= 0 ? '+' : ''}${num.toFixed(1)}`;
}

function fmt(n, digits = 3) {
  const x = Number(n);
  if (!isFinite(x)) return '—';
  return x.toFixed(digits);
}


function escape(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '\'': '&#39;' }[c])); }