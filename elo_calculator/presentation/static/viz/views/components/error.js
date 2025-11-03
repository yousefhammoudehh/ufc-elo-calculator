export function ErrorCard(err) {
    const msg = (err && err.message) ? escapeHtml(err.message) : 'Something went wrong.';
    return `
<div class="rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700">
<div class="font-semibold mb-1">Error</div>
<div>${msg}</div>
</div>`;
}

function escapeHtml(s) { return s.replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '\'': '&#39;' }[c])); }