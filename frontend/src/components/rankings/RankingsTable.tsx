import Link from "next/link";
import type { RankingListResponse } from "@/lib/types/ranking";
import { Pagination } from "@/components/ui/Pagination";

interface Props {
  rankings: RankingListResponse;
}

export function RankingsTable({ rankings }: Props) {
  const hasRd = rankings.data.some((r) => r.rd != null);

  return (
    <div>
      <div className="text-xs text-zinc-500 mb-3">
        As of <span className="font-data text-zinc-400">{rankings.as_of_date}</span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-800 text-left">
              <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium w-16">
                Rank
              </th>
              <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                Fighter
              </th>
              <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                Rating
              </th>
              {hasRd && (
                <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                  RD
                </th>
              )}
              <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                Last Fight
              </th>
            </tr>
          </thead>
          <tbody>
            {rankings.data.map((entry) => (
              <tr
                key={entry.fighter_id}
                className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors"
              >
                <td className="py-3 px-3 font-data text-zinc-400">{entry.rank}</td>
                <td className="py-3 px-3">
                  <Link
                    href={`/fighters/${entry.fighter_id}`}
                    className="text-zinc-100 hover:text-amber-400 transition-colors"
                  >
                    {entry.display_name}
                  </Link>
                </td>
                <td className="py-3 px-3 text-right font-data text-amber-400">
                  {entry.rating_mean.toFixed(1)}
                </td>
                {hasRd && (
                  <td className="py-3 px-3 text-right font-data text-zinc-500">
                    {entry.rd != null ? `\u00b1${entry.rd.toFixed(1)}` : "\u2014"}
                  </td>
                )}
                <td className="py-3 px-3 text-right font-data text-zinc-500">
                  {entry.last_fight_date ?? "\u2014"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pagination pagination={rankings.pagination} />
    </div>
  );
}
