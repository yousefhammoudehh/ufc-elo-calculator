async function fetchJSON(url) {
  const doFetch = async (u) => fetch(u);
  let res = await doFetch(url);
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

function qs(key) {
  const u = new URL(window.location.href);
  return u.searchParams.get(key);
}

function dateMinusDays(isoDateStr, days) {
  if (!isoDateStr) return isoDateStr;
  const d = new Date(isoDateStr);
  if (Number.isNaN(d.getTime())) return isoDateStr;
  d.setUTCDate(d.getUTCDate() - days);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// Baseline removed; starting ELO will be represented as a point one week before first fight

function renderChart(hist, overlayHist) {
  const ctx = document.getElementById('chart').getContext('2d');
  let labels = hist.points.map((p) => p.event_date || '');
  let values = hist.points.map((p) => p.elo_after ?? p.elo_before ?? null);
  const base = typeof hist.entry_elo === 'number' ? hist.entry_elo : null;

  // Insert a starting ELO point one week before the first fight
  if (base != null && labels.length > 0) {
    const preDate = dateMinusDays(labels[0], 7);
    labels = [preDate, ...labels];
    values = [base, ...values];
  }

  // If overlay is provided, render smooth overlapping comparison
  if (overlayHist) {
    const baseDates = (hist.points || []).map((p) => p.event_date || '').filter(Boolean);
    const otherDates = (overlayHist.points || []).map((p) => p.event_date || '').filter(Boolean);
    const preOther = typeof overlayHist.entry_elo === 'number' && otherDates.length ? dateMinusDays(otherDates[0], 7) : null;
    const preBase = base != null && baseDates.length ? dateMinusDays(baseDates[0], 7) : null;
    const allDates = Array.from(new Set([
      ...(preBase ? [preBase] : []),
      ...(preOther ? [preOther] : []),
      ...baseDates,
      ...otherDates,
    ])).sort();

    const byDateBase = new Map((hist.points || []).map((p) => [p.event_date || '', p]));
    const byDateOther = new Map((overlayHist.points || []).map((p) => [p.event_date || '', p]));
    const baseValues = allDates.map((d) => {
      if (preBase && d === preBase) return base;
      const p = byDateBase.get(d);
      return p ? (p.elo_after ?? p.elo_before ?? null) : null;
    });
    const overlayValues = allDates.map((d) => {
      if (preOther && d === preOther) return overlayHist.entry_elo;
      const p = byDateOther.get(d);
      return p ? (p.elo_after ?? p.elo_before ?? null) : null;
    });

    const datasets = [
      {
        label: `${hist.name} — ELO`,
        data: baseValues,
        borderColor: 'rgba(59, 163, 255, 1)',
        backgroundColor: 'rgba(59, 163, 255, 0.2)',
        tension: 0.35,
        spanGaps: true,
        pointRadius: allDates.map((d) => (preBase && d === preBase ? 6 : 2)),
      },
      {
        label: `${overlayHist.name} — start ${overlayHist.entry_elo ?? 'N/A'}`,
        data: overlayValues,
        borderColor: 'rgba(255, 159, 64, 1)',
        backgroundColor: 'rgba(255, 159, 64, 0.2)',
        tension: 0.35,
        spanGaps: true,
        pointRadius: allDates.map((d) => (preOther && d === preOther ? 6 : 2)),
      },
    ];

    if (window.__fighterChart) window.__fighterChart.destroy();
    window.__fighterChart = new Chart(ctx, {
      type: 'line',
      data: { labels: allDates, datasets },
      options: {
        plugins: {
          legend: { display: true },
          tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}` } },
        },
        scales: { y: { beginAtZero: false } },
      },
    });
    return;
  }

  // Single series chart
  const pointRadius = values.map((_, idx) => (idx === 0 ? 6 : 2));
  const pointBackgroundColor = values.map((_, idx) => (idx === 0 ? 'rgba(255, 215, 0, 1)' : 'rgba(59, 163, 255, 1)'));
  const datasets = [
    {
      label: `${hist.name} — ELO`,
      data: values,
      borderColor: 'rgba(59, 163, 255, 1)',
      backgroundColor: 'rgba(59, 163, 255, 0.2)',
      tension: 0.2,
      pointRadius,
      pointBackgroundColor,
    },
  ];

  if (window.__fighterChart) window.__fighterChart.destroy();
  window.__fighterChart = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      plugins: {
        legend: { display: true },
        tooltip: { callbacks: { label: (ctx) => `${ctx.dataset.label}: ${Number(ctx.parsed.y).toFixed(2)}` } },
      },
      scales: { y: { beginAtZero: false } },
    },
  });
}

function renderTable(hist) {
  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  const wcName = (code) => {
    const m = {
      101: 'Flyweight', 102: 'Bantamweight', 103: 'Featherweight', 104: 'Lightweight', 105: 'Welterweight', 106: 'Middleweight', 107: 'Light Heavyweight', 108: 'Heavyweight',
      201: 'Women Strawweight', 202: 'Women Flyweight', 203: 'Women Bantamweight', 204: 'Women Featherweight', 0: 'Openweight',
    };
    const n = Number(code);
    return Number.isFinite(n) ? (m[n] || '') : '';
  };
  for (const p of hist.points) {
    const tr = document.createElement('tr');
    const delta = p.delta != null ? (p.delta >= 0 ? `+${p.delta.toFixed(2)}` : p.delta.toFixed(2)) : '—';
    const outcomeBadge = (out) => {
      if (out === 'W') return '<span class="badge win">W</span>';
      if (out === 'L') return '<span class="badge loss">L</span>';
      if (out === 'D') return '<span class="badge draw">D</span>';
      if (out === 'NC') return '<span class="badge">NC</span>';
      return out || '';
    };
    const resultCell = `${outcomeBadge(p.result)}${p.is_title_fight ? ' <span class="badge title">Title</span>' : ''}`;
    tr.innerHTML = `
      <td>${p.event_date ?? ''}</td>
      <td>${p.opponent_id ? `<a href="/viz/fighter.html?id=${encodeURIComponent(p.opponent_id)}">${p.opponent_name ?? p.opponent_id}</a>` : (p.opponent_name ?? '')}</td>
      <td>${resultCell}</td>
      <td>${wcName(p.weight_class_code)}</td>
      <td>${typeof p.elo_before === 'number' ? p.elo_before.toFixed(2) : ''}</td>
      <td>${typeof p.elo_after === 'number' ? p.elo_after.toFixed(2) : ''}</td>
      <td>${delta}</td>
      <td>${p.rank_after ?? ''}</td>
      <td>${p.bout_id ? `<a href="/viz/bout.html?bout_id=${encodeURIComponent(p.bout_id)}">Details</a>` : ''}</td>
    `;
    tbody.appendChild(tr);
  }
}

async function resolveFighterId(query) {
  if (!query) return null;
  if (/\s/.test(query) || query.length < 8) {
    const results = await fetchJSON(`/fighters/search?q=${encodeURIComponent(query)}&limit=5`);
    if (results && results.length > 0) return results[0].fighter_id;
    return null;
  }
  return query;
}

async function loadPage() {
  const id = qs('id');
  if (!id) { window.location = '/viz/fighters.html'; return; }
  try {
    const hist = await fetchJSON(`/analytics/fighter-elo/${encodeURIComponent(id)}`);
    const curr = typeof hist.current_elo === 'number' ? hist.current_elo.toFixed(2) : '—';
    const peak = typeof hist.peak_elo === 'number' ? hist.peak_elo.toFixed(2) : '—';
    document.getElementById('fighterHeader').textContent = `${hist.name} — Current ${curr} (Peak ${peak})`;
    renderChart(hist);
    renderTable(hist);
    // Inline insight badges (Form, Momentum, Rates summary)
    try {
      const [fi, mom, rates, sos, qwins] = await Promise.all([
        fetchJSON(`/analytics/form?fighter_id=${encodeURIComponent(id)}&window=fights&n=5&half_life_days=180`),
        fetchJSON(`/analytics/momentum?fighter_id=${encodeURIComponent(id)}&k=6`),
        fetchJSON(`/analytics/rates?fighter_id=${encodeURIComponent(id)}`),
        fetchJSON(`/analytics/sos?fighter_id=${encodeURIComponent(id)}&window=days&n=365`),
        fetchJSON(`/analytics/quality-wins?fighter_id=${encodeURIComponent(id)}&elo_threshold=1600`),
      ]);
      const host = document.getElementById('insightBadges');
      if (host) {
        const tdPct = rates?.rates?.td_pct != null ? (rates.rates.td_pct * 100).toFixed(0) + '%' : '—';
        const ctrl = rates?.rates?.control_share != null ? (rates.rates.control_share * 100).toFixed(0) + '%' : '—';
        host.innerHTML = `
          <span class="badge" title="Recent residual vs expectation (decayed)">FI ${fi.fi != null ? (fi.fi > 0 ? '+' : '') + fi.fi.toFixed(2) : '—'}</span>
          <span class="badge" title="ELO slope over last 6 fights">Slope ${mom.slope_per_fight != null ? (mom.slope_per_fight > 0 ? '+' : '') + mom.slope_per_fight.toFixed(2) : '—'}</span>
          <span class="badge" title="Takedown success">TD% ${tdPct}</span>
          <span class="badge" title="Control share">Control ${ctrl}</span>
          <span class="badge" title="Quality Wins ≥1600 ELO">QoW ${qwins?.wins_q ?? '—'}/${qwins?.wins_total ?? '—'}</span>
          <span class="badge" title="Avg Opponent ELO last 365 days">SoS ${sos?.mean != null ? Math.round(sos.mean) : '—'}</span>
        `;
        // QoW threshold control
        const qbtn = document.getElementById('qwHdrApply');
        qbtn?.addEventListener('click', async () => {
          try {
            const thr = Number(document.getElementById('qwHdrThresh').value || 1600);
            const rw = await fetchJSON(`/analytics/quality-wins?fighter_id=${encodeURIComponent(id)}&elo_threshold=${encodeURIComponent(thr)}`);
            // Update QoW badge only
            const existing = Array.from(host.querySelectorAll('span.badge')).find(el => el.textContent?.startsWith('QoW'));
            if (existing) existing.textContent = `QoW ${rw?.wins_q ?? '—'}/${rw?.wins_total ?? '—'}`;
          } catch (e) { console.warn('QoW apply failed', e); }
        });
        // Header sparklines removed for clarity. Keep badges + explanations instead.
      }
    } catch (e) { console.warn('Inline insights failed', e); }
    // Load career totals
    try {
      const totals = await fetchJSON(`/analytics/fighter-career-stats/${encodeURIComponent(id)}`);
      renderCareerTotals(totals);
    } catch (e) { console.warn('Career totals failed', e); }
  } catch (e) {
    console.error('Failed to load fighter', e);
  }
}

async function overlay() {
  const q = document.getElementById('compareInput').value.trim();
  if (!q) return;
  const id = await resolveFighterId(q);
  if (!id) {
    alert('No match for comparison');
    return;
  }
  try {
    const baseId = qs('id');
    const [hist, other] = await Promise.all([
      fetchJSON(`/analytics/fighter-elo/${encodeURIComponent(baseId)}`),
      fetchJSON(`/analytics/fighter-elo/${encodeURIComponent(id)}`),
    ]);
    renderChart(hist, other);
  } catch (e) {
    console.error('Overlay failed', e);
  }
}

function renderCareerTotals(data) {
  const holder = document.getElementById('careerTotals');
  if (!holder) return;
  const kmap = {
    kd: 'Knockdowns', sig_strikes: 'Sig. Strikes', sig_strikes_thrown: 'Sig. Str Att', total_strikes: 'Total Strikes', total_strikes_thrown: 'Total Str Att', td: 'Takedowns', td_attempts: 'TD Att', sub_attempts: 'Sub Att', reversals: 'Reversals', control_time_sec: 'Control (s)', head_ss: 'Head SS', body_ss: 'Body SS', leg_ss: 'Leg SS', distance_ss: 'Distance SS', clinch_ss: 'Clinch SS', ground_ss: 'Ground SS'
  };
  // Build a single combined table: Stat | For (Total) | For (Avg) | Against (Total) | Against (Avg)
  const rows = Object.entries(kmap).map(([k, lab]) => {
    const ft = Number(data.totals_for?.[k] ?? 0);
    const fa = Number(data.averages_for?.[k] ?? 0);
    const at = Number(data.totals_against?.[k] ?? 0);
    const aa = Number(data.averages_against?.[k] ?? 0);
    return `<tr>
      <td>${lab}</td>
      <td style="text-align:right; font-variant-numeric:tabular-nums;">${ft}</td>
      <td style="text-align:right; font-variant-numeric:tabular-nums;">${fa.toFixed(2)}</td>
      <td style="text-align:right; font-variant-numeric:tabular-nums;">${at}</td>
      <td style="text-align:right; font-variant-numeric:tabular-nums;">${aa.toFixed(2)}</td>
    </tr>`;
  }).join('');
  holder.innerHTML = `<table style="width:100%; border-collapse:collapse;"><thead>
    <tr><th>Stat</th><th>For (Total)</th><th>For (Avg)</th><th>Against (Total)</th><th>Against (Avg)</th></tr>
  </thead><tbody>${rows}</tbody></table>`;

  // Year dropdown setup
  const byYearHolder = document.getElementById('careerByYear');
  const sel = document.getElementById('yearSelect');
  if (byYearHolder && sel) {
    const years = Object.keys(data.by_year || {}).map(Number).sort((a,b)=>a-b);
    sel.innerHTML = years.map(y => `<option value="${y}">${y}</option>`).join('');
    const renderYear = (y) => {
      const item = data.by_year?.[y];
      if (!item) { byYearHolder.innerHTML = '<div class="muted">No data for selected year</div>'; return; }
      const yrRows = Object.entries(kmap).map(([k,lab]) => `<tr>
        <td>${lab}</td>
        <td style="text-align:right; font-variant-numeric:tabular-nums;">${Number(item.totals_for?.[k] ?? 0)}</td>
        <td style="text-align:right; font-variant-numeric:tabular-nums;">${Number(item.totals_against?.[k] ?? 0)}</td>
      </tr>`).join('');
      byYearHolder.innerHTML = `<div style="flex:1; min-width:480px;">
        <div class="title" style="margin:0 0 6px;">${y} — fights: ${item.fights}</div>
        <table style="width:100%; border-collapse:collapse;"><thead>
          <tr><th>Stat</th><th>For (Total)</th><th>Against (Total)</th></tr>
        </thead><tbody>${yrRows}</tbody></table></div>`;
    };
    if (years.length) renderYear(years[years.length - 1]);
    sel.addEventListener('change', () => renderYear(Number(sel.value)));
  }
}

window.addEventListener('DOMContentLoaded', () => {
  document.getElementById('compareBtn').addEventListener('click', overlay);
  loadPage();
});
