async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const payload = await res.json();
  return payload.data !== undefined ? payload.data : payload;
}

async function resolveFighterId(q) {
  if (!q) return null;
  if (/\s/.test(q) || q.length < 8) {
    try {
      const results = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=5`);
      return results?.[0]?.fighter_id || null;
    } catch { return null; }
  }
  return q;
}

function hookupMode(elSel, elYear) {
  const update = () => { elYear.style.display = (elSel.value === 'year') ? 'inline-block' : 'none'; };
  elSel.addEventListener('change', update); update();
}

function renderResult(res) {
  const out = document.getElementById('out'); if (!out) return;
  const p1 = (res.P1 * 100).toFixed(2), p2 = (res.P2 * 100).toFixed(2);
  const odds1 = res.odds1 || {}; const odds2 = res.odds2 || {};
  const name1 = res.fighter1_name || res.fighter1_id;
  const name2 = res.fighter2_name || res.fighter2_id;
  const link1 = `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(res.fighter1_id)}">${name1}</a>`;
  const link2 = `<a class="link-muted" href="/viz/fighter.html?id=${encodeURIComponent(res.fighter2_id)}">${name2}</a>`;
  const mp1 = res.method_probs1 || {}; const mp2 = res.method_probs2 || {};
  const am1 = fmtAmerican(odds1.american); const am2 = fmtAmerican(odds2.american);
  const header = `<div style=\"grid-column:1 / span 3; margin-bottom:6px; font-weight:600;\">${name1}: ${am1} | ${name2}: ${am2} — <span class=\"muted\">Most likely:</span> ${res.winner_pred || '—'}</div>`;
  out.innerHTML = header + `
    <div class="panel">
      <div class="muted">${link1} — ELO ${Number(res.R1).toFixed(2)}</div>
      <div class="big">${p1}%</div>
      <div class="muted">Decimal ${odds1.decimal ?? '-'} · American ${odds1.american ?? '-'}</div>
  <div class="muted">By method:</div>
  <div>KO/TKO: ${((mp1['KO/TKO']||0)*100).toFixed(1)}% · TKO-DS: ${((mp1['TKO-DS']||0)*100).toFixed(1)}% · SUB: ${((mp1['SUB']||0)*100).toFixed(1)}% · DEC: ${((mp1['DEC']||0)*100).toFixed(1)}%</div>
    </div>
    <div class="vs">VS</div>
    <div class="panel">
      <div class="muted">${link2} — ELO ${Number(res.R2).toFixed(2)}</div>
      <div class="big">${p2}%</div>
      <div class="muted">Decimal ${odds2.decimal ?? '-'} · American ${odds2.american ?? '-'}</div>
  <div class="muted">By method:</div>
  <div>KO/TKO: ${((mp2['KO/TKO']||0)*100).toFixed(1)}% · TKO-DS: ${((mp2['TKO-DS']||0)*100).toFixed(1)}% · SUB: ${((mp2['SUB']||0)*100).toFixed(1)}% · DEC: ${((mp2['DEC']||0)*100).toFixed(1)}%</div>
    </div>`;
  // summary now rendered above panels
}

function fmtAmerican(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return n > 0 ? `+${n}` : `${n}`;
}

window.addEventListener('DOMContentLoaded', () => {
  const f1Mode = document.getElementById('f1Mode');
  const f2Mode = document.getElementById('f2Mode');
  const f1Year = document.getElementById('f1Year');
  const f2Year = document.getElementById('f2Year');
  hookupMode(f1Mode, f1Year);
  hookupMode(f2Mode, f2Year);

  // Attach floating autocomplete dropdowns
  setupAutocomplete('f1Input');
  setupAutocomplete('f2Input');

  // Enter handlers
  const compute = () => document.getElementById('computeBtn').click();
  document.getElementById('f1Input').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); compute(); }});
  document.getElementById('f2Input').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); compute(); }});

  document.getElementById('computeBtn').addEventListener('click', async () => {
    const in1 = document.getElementById('f1Input');
    const in2 = document.getElementById('f2Input');
    const a = (in1.value || '').trim();
    const b = (in2.value || '').trim();
    if (!a || !b) { alert('Enter both fighters'); return; }
    const fid1 = in1.dataset.fighterId || await resolveFighterId(a);
    const fid2 = in2.dataset.fighterId || await resolveFighterId(b);
    if (!fid1 || !fid2) { alert('Could not resolve fighter(s)'); return; }
    const m1 = f1Mode.value, m2 = f2Mode.value;
    const y1 = m1 === 'year' ? Number(f1Year.value || 0) : '';
    const y2 = m2 === 'year' ? Number(f2Year.value || 0) : '';
    try {
      showLoader();
      const explain = document.getElementById('explainChk').checked;
      const adj = document.getElementById('adjMode')?.value || 'base';
      const fr = document.getElementById('chkFiveRound')?.checked ? 'true' : '';
      const ti = document.getElementById('chkTitle')?.checked ? 'true' : '';
      const url = `/analytics/h2h?fighter1=${encodeURIComponent(fid1)}&fighter2=${encodeURIComponent(fid2)}&mode1=${encodeURIComponent(m1)}&mode2=${encodeURIComponent(m2)}${y1 ? `&year1=${y1}` : ''}${y2 ? `&year2=${y2}` : ''}&adjust=${encodeURIComponent(adj)}${fr ? `&five_round=${fr}` : ''}${ti ? `&title=${ti}` : ''}${explain ? '&explain=true' : ''}`;
      const res = await fetchJSON(url);
      // Keep pair for explain fetches
      window.__h2hLastIds = [res.fighter1_id, res.fighter2_id];
      renderResult(res);
      renderExplain2(res.explain);
    } catch (e) {
      console.error('Failed to compute h2h', e); alert('Failed to compute');
    } finally { hideLoader(); }
  });
});

// No EWMA HL control in UI; backend uses default (180 days)

function setupAutocomplete(inputId) {
  const inp = document.getElementById(inputId);
  if (!inp) return;
  const box = document.createElement('div');
  box.className = 'ac-list';
  document.body.appendChild(box);
  function positionBox() {
    const r = inp.getBoundingClientRect();
    box.style.left = `${Math.round(r.left)}px`;
    box.style.top = `${Math.round(r.bottom)}px`;
    box.style.width = `${Math.round(r.width)}px`;
  }
  let timer = null;
  async function query(q) {
    if (!q || q.length < 2) { box.style.display = 'none'; box.innerHTML = ''; return; }
    try {
      const items = await fetchJSON(`/fighters/search?q=${encodeURIComponent(q)}&limit=6`);
      if (!items || !items.length) { box.style.display = 'none'; box.innerHTML = ''; return; }
      box.innerHTML = items.map(it => `<div class="ac-item" data-id="${it.fighter_id}">${it.name || it.fighter_id}</div>`).join('');
      positionBox();
      box.style.display = 'block';
    } catch { box.style.display = 'none'; }
  }
  inp.addEventListener('input', () => {
    inp.dataset.fighterId = '';
    clearTimeout(timer);
    timer = setTimeout(() => query(inp.value.trim()), 180);
  });
  inp.addEventListener('focus', () => { if (box.innerHTML) { positionBox(); box.style.display = 'block'; } });
  window.addEventListener('resize', () => { if (box.style.display === 'block') positionBox(); });
  window.addEventListener('scroll', () => { if (box.style.display === 'block') positionBox(); }, true);
  document.addEventListener('click', (e) => { if (!box.contains(e.target) && e.target !== inp) box.style.display = 'none'; });
  box.addEventListener('click', (e) => {
    const el = e.target.closest('.ac-item'); if (!el) return;
    inp.value = el.textContent || el.dataset.id || '';
    if (el.dataset.id) {
      inp.dataset.fighterId = el.dataset.id;
    }
    box.style.display = 'none';
  });
}

function showLoader() {
  const card = document.querySelector('.card');
  if (!card) return;
  if (card.querySelector(':scope > .loader-overlay')) return;
  const overlay = document.createElement('div');
  overlay.className = 'loader-overlay';
  overlay.innerHTML = '<div class="loader"><span class="spinner"></span><span>Computing…</span></div>';
  card.appendChild(overlay);
}
function hideLoader() {
  const card = document.querySelector('.card');
  const ov = card ? card.querySelector(':scope > .loader-overlay') : null;
  if (ov) ov.remove();
}

// New explain renderer that fetches missing factors and shows meanings
async function renderExplain2(ex) {
  const host = document.getElementById('explainPanel'); if (!host) return;
  if (!ex) { host.innerHTML = ''; return; }
  let fi = ex.form_index_delta, sos = ex.sos_mean_delta, cons = ex.consistency_delta;
  try {
    if (fi == null || sos == null || cons == null) {
      const ids = window.__h2hLastIds || [];
      if (ids.length === 2) {
        const [f1, f2] = ids;
        const [fi1, fi2] = await Promise.all([
          fetchJSON(`/analytics/form?fighter_id=${encodeURIComponent(f1)}&window=fights&n=5&half_life_days=180`),
          fetchJSON(`/analytics/form?fighter_id=${encodeURIComponent(f2)}&window=fights&n=5&half_life_days=180`),
        ]);
        const [s1, s2] = await Promise.all([
          fetchJSON(`/analytics/sos?fighter_id=${encodeURIComponent(f1)}&window=days&n=365`),
          fetchJSON(`/analytics/sos?fighter_id=${encodeURIComponent(f2)}&window=days&n=365`),
        ]);
        const [c1, c2] = await Promise.all([
          fetchJSON(`/analytics/consistency-versatility?fighter_id=${encodeURIComponent(f1)}&k=6`),
          fetchJSON(`/analytics/consistency-versatility?fighter_id=${encodeURIComponent(f2)}&k=6`),
        ]);
        fi = (fi1?.fi ?? 0) - (fi2?.fi ?? 0);
        sos = (s1?.mean ?? 0) - (s2?.mean ?? 0);
        cons = (c1?.sd_elo_delta ?? 0) - (c2?.sd_elo_delta ?? 0);
      }
    }
  } catch {}
  const rows = [];
  const add = (k, v, meaning) => rows.push(
    `<tr>`+
      `<td style="padding:4px 6px; font-size:0.95em;">${k}</td>`+
      `<td style="padding:4px 6px; text-align:right; font-size:0.95em;">${(v==null)?'—':(typeof v==='number'?v.toFixed(3):String(v))}</td>`+
      `<td class="muted" style="padding:4px 6px; font-size:0.95em;">${meaning}</td>`+
    `</tr>`
  );
  add('ELO gap (A-B)', ex.elo_gap, 'Rating difference; negative favors Fighter B');
  add('Form Index Δ', fi, 'Recent over/under‑performance (A − B)');
  add('SoS mean Δ', sos, 'Recent opponent difficulty (A − B)');
  add('Consistency Δ (SD ΔELO)', cons, 'Volatility in recent ELO swings (A − B)');
  const mode = (ex && ex.adjust) || 'base';
  const li = (title, body, key) => `<div style="margin:6px 0;"><strong>${title}</strong>${mode===key?'<span class="badge" style="background:#1f2937;border:1px solid #2b3a4d;margin-left:6px;">active</span>':''}<div class="muted">${body}</div></div>`;
  const modesBlock = `
    <div style="line-height:1.5; margin-top:8px;">
      <div style="margin-bottom:4px;"><strong>Probability modes</strong></div>
      ${li('Base (ELO only)', 'Uses the ELO gap at the chosen snapshot. Probability = logistic(E1−E2). Transparent and stable.', 'base')}
      ${li('Recency blend', 'Blends current ELO with a recent ELO track before the logistic. Adds gentle “recent form” influence.', 'window')}
      ${li('Light nudge', 'Converts small diffs in Form/SoS/Consistency into a capped ELO offset. Keeps ELO as backbone; returns per‑factor ELO contributions.', 'nudge')}
      ${li('Calibrated', 'Adds a small learned correction on top of ELO and applies calibration. Best reliability; also returns per‑factor ELO contributions.', 'meta')}
      ${li('Best (blend)', 'Blends recency, nudge and calibration for a pragmatic estimate with caps and calibration.', 'best')}
    </div>`;
  host.innerHTML = `<div class="card" style="margin-top:8px;">
    <div class="title" style="margin:0 0 6px; font-weight:600;">Explain factors</div>
    <table style="width:100%; border-collapse:collapse;">
      <thead>
        <tr>
          <th style="padding:4px 6px; text-align:left; font-size:0.92em;">Factor</th>
          <th style="padding:4px 6px; text-align:right; font-size:0.92em;">Value</th>
          <th style="padding:4px 6px; text-align:left; font-size:0.92em;">Meaning</th>
        </tr>
      </thead>
      <tbody>${rows.join('')}</tbody>
    </table>
    ${modesBlock}
  </div>`;
}
function renderExplain(ex) {
  const host = document.getElementById('explainPanel'); if (!host) return;
  if (!ex) { host.innerHTML = ''; return; }
  const rows = [];
  const add = (k, v, meaning) => rows.push(
    `<tr>`+
      `<td style="padding:4px 6px; font-size:0.95em;">${k}</td>`+
      `<td style="padding:4px 6px; text-align:right; font-size:0.95em;">${(v==null)?'—':(typeof v==='number'?v.toFixed(3):String(v))}</td>`+
      `<td class="muted" style="padding:4px 6px; font-size:0.95em;">${meaning}</td>`+
    `</tr>`
  );
  add('ELO gap (A-B)', ex.elo_gap, 'Rating difference; negative favors Fighter B');
  add('Form Index Δ', ex.form_index_delta, 'Recent over/under‑performance (A − B)');
  add('SoS mean Δ', ex.sos_mean_delta, 'Recent opponent difficulty (A − B)');
  add('Consistency Δ (SD ΔELO)', ex.consistency_delta, 'Volatility in recent ELO swings (A − B)');
  const mode = (ex && ex.adjust) || 'base';
  const li = (title, body, key) => `<div style="margin:6px 0;"><strong>${title}</strong>${mode===key?'<span class="badge" style="background:#1f2937;border:1px solid #2b3a4d;margin-left:6px;">active</span>':''}<div class="muted">${body}</div></div>`;
  const modesBlock = `
    <div style=\"line-height:1.5; margin-top:8px;\">
      <div style=\"margin-bottom:4px;\"><strong>Probability modes</strong></div>
      ${li('Base (ELO only)', 'Uses the ELO gap at the chosen snapshot. Probability = logistic(E1−E2). Transparent and stable.', 'base')}
      ${li('Recency blend', 'Blends current ELO with a recent ELO track before the logistic. Adds gentle “recent form” influence.', 'window')}
      ${li('Light nudge', 'Converts small diffs in Form/SoS/Consistency into a capped ELO offset. Keeps ELO as backbone; returns per‑factor ELO contributions.', 'nudge')}
      ${li('Calibrated', 'Adds a small learned correction on top of ELO and applies calibration. Best reliability; also returns per‑factor ELO contributions.', 'meta')}
      ${li('Best (blend)', 'Blends recency, nudge and calibration for a pragmatic estimate with caps and calibration.', 'best')}
    </div>`;
  const text = ``;
  host.innerHTML = `<div class=\"card\" style=\"margin-top:8px;\">\n    <div class=\"title\" style=\"margin:0 0 6px; font-weight:600;\">Explain factors</div>\n    <table style=\"width:100%; border-collapse:collapse;\">\n      <thead>\n        <tr>\n          <th style=\"padding:4px 6px; text-align:left; font-size:0.92em;\">Factor</th>\n          <th style=\"padding:4px 6px; text-align:right; font-size:0.92em;\">Value</th>\n          <th style=\"padding:4px 6px; text-align:left; font-size:0.92em;\">Meaning</th>\n        </tr>\n      </thead>\n      <tbody>${rows.join('')}</tbody>\n    </table>\n    ${modesBlock}\n  </div>`;
}
