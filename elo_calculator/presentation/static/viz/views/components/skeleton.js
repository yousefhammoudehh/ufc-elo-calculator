export function SkeletonCard(lines = 3) {
    const shimmer = 'relative overflow-hidden before:absolute before:inset-0 before:-translate-x-full before:animate-[shimmer_1.8s_infinite] before:bg-gradient-to-r before:from-transparent before:via-white/60 before:to-transparent';
    const row = `<div class="h-4 bg-slate-200 rounded ${shimmer}"></div>`;
    return `<div class="space-y-3">${Array.from({ length: lines }).map(() => row).join('')}</div>`;
}

export function SkeletonTable(rows = 8, cols = 4) {
    const shimmer = 'relative overflow-hidden before:absolute before:inset-0 before:-translate-x-full before:animate-[shimmer_2s_infinite] before:bg-gradient-to-r before:from-transparent before:via-white/60 before:to-transparent';
    const r = () => `<tr>${'<td class="py-2"><div class="h-3 bg-slate-200 rounded ' + shimmer + '"></div></td>'.repeat(cols)}</tr>`;
    return `<div class="overflow-x-auto"><table class="w-full">${Array.from({ length: rows }).map(r).join('')}</table></div>`;
}