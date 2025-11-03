// Tiny client for our analytics API. Unwraps the envelope { status_code, message, data }.
export const Config = {
    BASE_URL: '/', // same origin
    headers() { return { 'Content-Type': 'application/json' }; }
};

async function http(path, params) {
    const url = new URL(path, window.location.origin);
    if (params) Object.entries(params).forEach(([k, v]) => v !== undefined && url.searchParams.set(k, v));
    const res = await fetch(url.toString(), { headers: Config.headers() });
    if (!res.ok) throw await toError(res);
    const json = await res.json();
    if (json.status_code && json.status_code !== 200) {
        const e = new Error(json.message || 'Request failed');
        e.payload = json; throw e;
    }
    return json.data ?? json; // Some endpoints might return raw arrays
}

async function toError(res) {
    const t = await res.text().catch(() => '');
    let msg = t || res.statusText;
    try { const j = JSON.parse(t); msg = j.message || JSON.stringify(j); } catch (_) { }
    return new Error(`${res.status} ${res.statusText}: ${msg}`);
}

export const api = {
    latestEventElo: () => http('/analytics/latest-event-elo'),
    latestEventShock: () => http('/analytics/latest-event-shock'),
    eventShock: () => http('/analytics/event-shock'), // latest when no event_id is passed
    formTop: (p) => http('/analytics/form-top', p),
    formSeries: (fighter_id) => http('/analytics/form', { fighter_id, window: 'fights', n: 5 }),
    topElo: (limit = 10) => http('/analytics/top-elo', { limit }),
    topPeakElo: (limit = 10) => http('/analytics/top-peak-elo', { limit }),
    eventsShockTop: (p) => http('/analytics/events-shock-top', p),
    topElo: (limit = 10) => http('/analytics/top-elo', { limit }),
    topPeakElo: (limit = 10) => http('/analytics/top-peak-elo', { limit }),
    eventsShockTop: (p) => http('/analytics/events-shock-top', p),
};