/* global window, document, location, fetch */

async function fetchJSON(url) {
  const res = await fetch(url);
  if (res.ok) return res.json();
  if (res.status === 404 && !url.startsWith('/api')) return fetch('/api' + url).then(r => r.json());
  throw new Error('Request failed ' + res.status);
}

function q(sel){ return document.querySelector(sel); }
function fmt2(x){ if (x === null || x === undefined) return '-'; return Number(x).toFixed(2); }
function num(v){ const n = Number(v); return Number.isFinite(n) ? n : NaN; }
function badgeOutcome(outcome){
  const map = { WIN: 'W', LOSS: 'L', DRAW: 'D', NO_CONTEST: 'NC' };
  return map[outcome] || outcome || '';
}

function setMeta(details){
  const evt = details.event_name ? `Event: ${details.event_name}` : (details.event_date ? `Event date: ${details.event_date}` : '');
  q('#eventMeta').textContent = evt;
  const parts = [];
  if (details.method) parts.push(`Method: ${details.method}`);
  if (details.rounds_scheduled) parts.push(`${details.rounds_scheduled} Rounds`);
  if (details.round_num) parts.push(`Ended R${details.round_num}`);
  if (details.time_sec !== null && details.time_sec !== undefined) parts.push(`${details.time_sec}s`);
  q('#boutMeta').textContent = parts.join(' • ');
}

function dynamicSigThreshold(side){
  // Scale significance with K_final when available; fallback to a floor.
  const kb = side.k_breakdown || {};
  const kf = Number(kb.K_final || side.K || 0);
  const thresh = Math.max(10, 0.35 * (Number.isFinite(kf) ? kf : 0));
  return thresh;
}

function setNamesAndElo(side1, side2){
  q('#f1Name').textContent = side1.fighter.name;
  q('#f2Name').textContent = side2.fighter.name;
  q('#f1Outcome').textContent = badgeOutcome(side1.outcome);
  q('#f2Outcome').textContent = badgeOutcome(side2.outcome);
  const d1 = side1.elo_delta >= 0 ? `+${fmt2(side1.elo_delta)}` : fmt2(side1.elo_delta);
  const d2 = side2.elo_delta >= 0 ? `+${fmt2(side2.elo_delta)}` : fmt2(side2.elo_delta);
  const s1 = Math.abs(Number(side1.elo_delta || 0)) >= dynamicSigThreshold(side1) ? ' — significant' : '';
  const s2 = Math.abs(Number(side2.elo_delta || 0)) >= dynamicSigThreshold(side2) ? ' — significant' : '';
  q('#f1Elo').textContent = `${fmt2(side1.elo_before)} \u2192 ${fmt2(side1.elo_after)} (${d1})${s1}`;
  q('#f2Elo').textContent = `${fmt2(side2.elo_before)} \u2192 ${fmt2(side2.elo_after)} (${d2})${s2}`;
}

function buildComparisonTable(side1, side2){
  const rows = [
    ['KD', side1.kd, side2.kd],
    ['Sig. Str', side1.sig_strikes, side2.sig_strikes],
    ['Sig. Str Att', side1.sig_strikes_thrown, side2.sig_strikes_thrown],
    ['Total Str', side1.total_strikes, side2.total_strikes],
    ['Total Str Att', side1.total_strikes_thrown, side2.total_strikes_thrown],
    ['TD', side1.td, side2.td],
    ['TD Att', side1.td_attempts, side2.td_attempts],
    ['Sub Att', side1.sub_attempts, side2.sub_attempts],
    ['Reversals', side1.reversals, side2.reversals],
    ['Control (s)', side1.control_time_sec, side2.control_time_sec],
    ['Head SS', side1.head_ss, side2.head_ss],
    ['Body SS', side1.body_ss, side2.body_ss],
    ['Leg SS', side1.leg_ss, side2.leg_ss],
    ['Distance SS', side1.distance_ss, side2.distance_ss],
    ['Clinch SS', side1.clinch_ss, side2.clinch_ss],
    ['Ground SS', side1.ground_ss, side2.ground_ss],
    ['Accuracy (%)',
      side1.strike_accuracy != null ? (Number(side1.strike_accuracy) * 100).toFixed(2) : '-',
      side2.strike_accuracy != null ? (Number(side2.strike_accuracy) * 100).toFixed(2) : '-'],
    ['Performance Score', fmt2(side1.ps), fmt2(side2.ps)],
  ];
  const header = `<tr><th>Stat</th><th>${side1.fighter.name}</th><th>${side2.fighter.name}</th></tr>`;
  const body = rows.map(([k, v1, v2]) => {
    let d1 = v1;
    let d2 = v2;
    let n1 = num(v1);
    let n2 = num(v2);
    // Treat '-' as NaN for comparison
    if (!Number.isFinite(n1)) n1 = NaN;
    if (!Number.isFinite(n2)) n2 = NaN;
    const bold1 = Number.isFinite(n1) && (!Number.isFinite(n2) || n1 > n2);
    const bold2 = Number.isFinite(n2) && (!Number.isFinite(n1) || n2 > n1);
    if (k === 'Performance Score') {
      const lead1 = Number(side1.ps) > Number(side2.ps);
      const lead2 = Number(side2.ps) > Number(side1.ps);
      if (lead1) d1 = `${fmt2(side1.ps)} <span class="badge">leader</span>`;
      if (lead2) d2 = `${fmt2(side2.ps)} <span class="badge">leader</span>`;
    }
    return `<tr><th>${k}</th><td>${bold1 ? '<strong>' : ''}${d1 ?? '-'}${bold1 ? '</strong>' : ''}</td><td>${bold2 ? '<strong>' : ''}${d2 ?? '-'}${bold2 ? '</strong>' : ''}</td></tr>`;
  }).join('');
  q('#cmpTable').innerHTML = header + body;
}

function setCalc(elId, side){
  let calc = `E=${fmt2(side.E)}, Y=${fmt2(side.Y)}, K=${fmt2(side.K)} \u2192\nR_after = ${fmt2(side.elo_before)} + ${fmt2(side.K)} * (${fmt2(side.Y)} - ${fmt2(side.E)}) = ${fmt2(side.elo_after)}`;
  if (side.k_breakdown) {
    const kb = side.k_breakdown;
    const lines = [
      `K breakdown:`,
      `  base K0: ${fmt2(kb.base_K0)}`,
      `  method class: ${kb.method_class}`,
      `  multipliers -> rounds: ${fmt2(kb.mult_rounds)}, method: ${fmt2(kb.mult_method)}, experience: ${fmt2(kb.mult_experience)}, recency: ${fmt2(kb.mult_recency)}, finish: ${fmt2(kb.mult_finish)}`,
      kb.finish_u != null ? `  finish u: ${fmt2(kb.finish_u)}` : null,
      `  K final: ${fmt2(kb.K_final)}`,
    ].filter(Boolean);
    calc += `\n\n${lines.join('\n')}`;
  }
  q(elId).textContent = calc;
}

function setPlainEnglish(elId, side){
  const ePct = side.E != null ? (Number(side.E) * 100).toFixed(0) : '-';
  const yPct = side.Y != null ? (Number(side.Y) * 100).toFixed(0) : '-';
  const kb = side.k_breakdown || {};
  const parts = [];
  if (ePct !== '-') parts.push(`Before the fight, expectation E was about ${ePct}%.`);
  if (side.k_breakdown && kb.method_class) parts.push(`Result method was ${kb.method_class}, which pushes the target result Y higher for the winner (and lower for the loser).`);
  if (yPct !== '-') parts.push(`That yields a target Y around ${yPct}%.`);
  const drivers = [];
  if (kb.mult_rounds && Number(kb.mult_rounds) !== 1) drivers.push('rounds');
  if (kb.mult_method && Number(kb.mult_method) !== 1) drivers.push('method');
  if (kb.mult_experience && Number(kb.mult_experience) !== 1) drivers.push('experience');
  if (kb.mult_recency && Number(kb.mult_recency) !== 1) drivers.push('recency');
  if (kb.mult_finish && Number(kb.mult_finish) !== 1) drivers.push('finish time');
  if (drivers.length) parts.push(`K scales the change based on ${drivers.join(', ')}.`);
  parts.push(`Putting it together: R_after = R_before + K * (Y - E).`);
  const delta = side.elo_delta != null ? Number(side.elo_delta).toFixed(2) : null;
  if (delta) parts.push(`Here that’s a ${delta.startsWith('-') ? '' : '+'}${delta} move${Math.abs(Number(delta)) >= 15 ? ' — significant for a single fight' : ''}.`);
  q(elId).textContent = parts.join(' ');
}

async function main(){
  const params = new URLSearchParams(location.search);
  const boutId = params.get('bout_id');
  if (!boutId) {
    document.body.innerHTML = '<p style="color:#b91c1c">Missing bout_id query parameter.</p>';
    return;
  }
  const data = await fetchJSON(`/bouts/${encodeURIComponent(boutId)}/details`);
  const details = data.data || data; // support envelope
  setMeta(details);
  // Division label if available
  const divName = (code) => {
    const m = {101:'Flyweight',102:'Bantamweight',103:'Featherweight',104:'Lightweight',105:'Welterweight',106:'Middleweight',107:'Light Heavyweight',108:'Heavyweight',201:'Women Strawweight',202:'Women Flyweight',203:'Women Bantamweight',204:'Women Featherweight',0:'Openweight'};
    const n = Number(details.weight_class_code);
    return Number.isFinite(n) ? (m[n] || '') : '';
  };
  const wcl = q('#wcLabel'); if (wcl) wcl.textContent = divName(details.weight_class_code);
  setNamesAndElo(details.side1, details.side2);
  buildComparisonTable(details.side1, details.side2);
  // Plain-English PS explainer
  q('#psExplainer').textContent = `Performance score is a single number from 0 to 1 that sums up who did more and better work — weighing knockdowns, effective strikes, control time, takedowns, submission attempts, accuracy and overall control. Higher is better. Here: ${details.side1.fighter.name} ${fmt2(details.side1.ps)} vs ${details.side2.fighter.name} ${fmt2(details.side2.ps)}.`;
  // ELO details (compact lists per fighter)
  function renderEloDetails(side, sel) {
    const host = q(sel); if (!host || !side) return;
    const name = side.fighter?.name || 'Fighter';
    const ePct = side.E != null ? (Number(side.E) * 100).toFixed(1) + '%' : '—';
    const yPct = side.Y != null ? (Number(side.Y) * 100).toFixed(1) + '%' : '—';
    const before = fmt2(side.elo_before);
    const after = fmt2(side.elo_after);
    const delta = side.elo_delta != null ? (Number(side.elo_delta) >= 0 ? `+${fmt2(side.elo_delta)}` : fmt2(side.elo_delta)) : '—';
    const kUsed = fmt2(side.K);
    const eq = `R_after = R_before + K * (Y - E)`;
    host.innerHTML = `
      <div class="title" style="margin:0 0 6px;">${name}</div>
      <ul style="margin:0; padding-left:18px; line-height:1.5;">
        <li><strong>Expectation (E)</strong>: ${ePct}</li>
        <li><strong>Target (Y)</strong>: ${yPct}</li>
        <li><strong>K used</strong>: ${kUsed}</li>
        <li><strong>Before → After</strong>: ${before} → ${after} (<span class="${Number(side.elo_delta||0)>=0?'delta pos':'delta neg'}">${delta}</span>)</li>
        <li><strong>Formula</strong>: <span title="${eq}">${eq}</span></li>
      </ul>
    `;
  }
  renderEloDetails(details.side1, '#elo1');
  renderEloDetails(details.side2, '#elo2');

  // Performance score details (per fighter)
  function renderPSDetails(side, sel) {
    const host = q(sel); if (!host || !side) return;
    const name = side.fighter?.name || 'Fighter';
    const sig = side.sig_strikes != null ? Number(side.sig_strikes) : null;
    const siga = side.sig_strikes_thrown != null ? Number(side.sig_strikes_thrown) : null;
    const sigpct = siga && siga > 0 ? ((sig / siga) * 100).toFixed(1) + '%' : (side.strike_accuracy != null ? (Number(side.strike_accuracy) * 100).toFixed(1) + '%' : '—');
    const kd = side.kd ?? 0;
    const ctrl = side.control_time_sec ?? 0;
    const td = side.td ?? 0;
    const tda = side.td_attempts ?? 0;
    const clinch = side.clinch_ss ?? 0;
    const ground = side.ground_ss ?? 0;
    host.innerHTML = `
      <div class="title" style="margin:0 0 6px;">${name}</div>
      <ul style="margin:0; padding-left:18px; line-height:1.5;">
        <li><strong>PS</strong>: ${fmt2(side.ps)}</li>
        <li><strong>Sig. striking</strong>: ${sig ?? '—'} / ${siga ?? '—'} (${sigpct})</li>
        <li><strong>Knockdowns</strong>: ${kd}</li>
        <li><strong>Control time</strong>: ${ctrl}s (includes clinch/ground)</li>
        <li><strong>Takedowns</strong>: ${td}/${tda}</li>
        <li><strong>Clinch SS</strong>: ${clinch}, <strong>Ground SS</strong>: ${ground}</li>
      </ul>
    `;
  }
  // Removed per request: basic PS bullets per fighter
  // Performance score "shares" bullets (global per bout)
  function renderPSSharesSingle(details, sel) {
    const host = q(sel); if (!host) return;
    const kb1 = details.side1?.k_breakdown || {};
    const shares = kb1.PS_SHARES || kb1.ps_shares || null; // backend attaches global shares
    const fmt = (x, d = 3) => (x == null ? '—' : Number(x).toFixed(d));
    const tip = (text, html) => `<span title="${String(text).replaceAll('"','\"')}">${html}</span>`;
    // Section removed: do not render anything
    host.innerHTML = '';
  }
  renderPSSharesSingle(details, '#ps_shares');
  // K-factor details as compact bullet lists
  function renderKDetails(side, sel) {
    const host = q(sel);
    if (!host || !side) return;
    const kb = side.k_breakdown || {};
    const fmt = (x, digits = 3) => (x == null ? '—' : Number(x).toFixed(digits));
    const items = [];
    const tip = (text, html) => `<span title="${String(text).replaceAll('"','\"')}">${html}</span>`;
    const header = `<div class="title" style="margin:0 0 6px;">${side.fighter?.name || 'Fighter'}</div>`;
    items.push(tip('Starting K prior to multipliers (baseline for all bouts).', `<strong>Base K0</strong>: ${fmt(kb.base_K0, 2)}`));
    if (kb.rounds_scheduled != null || kb.schedule_total_seconds != null) {
      items.push(tip('Rounds multiplier adjusts K for scheduled duration (e.g., 5x5 titles vs 3x5). Longer schedules increase K modestly.', `<strong>Rounds</strong>: ${kb.rounds_scheduled ?? '—'} segments, total ${kb.schedule_total_seconds ?? '—'}s → mult ${fmt(kb.mult_rounds)}`));
    } else if (kb.mult_rounds != null) {
      items.push(tip('Rounds multiplier adjusts K for scheduled duration.', `<strong>Rounds multiplier</strong>: ${fmt(kb.mult_rounds)}`));
    }
    if (kb.method_class != null || kb.mult_method != null) {
      items.push(tip('Method multiplier raises K for decisive outcomes (e.g., KO/Sub) and lowers for decisions.', `<strong>Method</strong>: ${kb.method_class ?? '—'} → mult ${fmt(kb.mult_method)}`));
    }
    if (kb.ufc_fights_before != null || kb.mult_experience != null) {
      items.push(tip('Experience multiplier tapers K as fighters accrue UFC fights (stabilization).', `<strong>Experience</strong>: UFC fights before=${kb.ufc_fights_before ?? '—'} → mult ${fmt(kb.mult_experience)}`));
    }
    if (kb.days_since_last_fight != null || kb.mult_recency != null) {
      items.push(tip('Recency multiplier accounts for layoff vs. quick turnaround; long layoffs can slightly reduce K.', `<strong>Recency</strong>: days since last fight=${kb.days_since_last_fight ?? '—'} → mult ${fmt(kb.mult_recency)}`));
    }
    if (kb.mult_finish != null) {
      const extra = kb.finish_u != null ? ` (u=${fmt(kb.finish_u)})` : '';
      items.push(tip('Finish-time boost increases K for earlier stoppages; u is normalized time (0=instant, 1=final bell).', `<strong>Finish-time</strong>: mult ${fmt(kb.mult_finish)}${extra}`));
    }
    const capNote = kb.base_K0 != null ? ` (safety cap ~1.5×K0=${(1.5 * Number(kb.base_K0)).toFixed(2)})` : '';
    items.push(tip('Final K after all multipliers and title adjustments; bounded by a safety cap.', `<strong>Final K</strong>: ${fmt(side.K)}${capNote}`));
    host.innerHTML = `${header}<ul style="margin:0; padding-left:18px; line-height:1.5;">${items.map((i) => `<li>${i}</li>`).join('')}</ul>`;
  }
  renderKDetails(details.side1, '#k1');
  renderKDetails(details.side2, '#k2');
}

main().catch(err => {
  console.error(err);
  document.body.innerHTML += `<p style="color:#b91c1c">Failed to load bout details: ${String(err)}</p>`;
});
