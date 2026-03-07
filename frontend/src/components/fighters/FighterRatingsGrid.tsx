import type { FighterRating } from "@/lib/types/fighter";

interface Props {
  ratings: FighterRating[];
}

function formatSystemKey(key: string): string {
  return key
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function FighterRatingsGrid({ ratings }: Props) {
  if (ratings.length === 0) return null;

  return (
    <div>
      <h2 className="text-lg font-semibold text-zinc-200 mb-3">Current Ratings</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {ratings.map((r) => (
          <div
            key={r.system_key}
            className="bg-zinc-900 border border-zinc-800 rounded-lg p-4"
          >
            <p className="text-xs text-zinc-500 uppercase tracking-wider mb-2 truncate">
              {formatSystemKey(r.system_key)}
            </p>
            <p className="text-2xl font-semibold font-data text-amber-400">
              {r.rating_mean.toFixed(1)}
            </p>
            <div className="mt-2 flex items-center gap-3 text-xs text-zinc-500">
              {r.rd != null && (
                <span className="font-data">&plusmn;{r.rd.toFixed(1)}</span>
              )}
              <span className="font-data">
                Peak: {r.peak_rating.toFixed(1)}
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
