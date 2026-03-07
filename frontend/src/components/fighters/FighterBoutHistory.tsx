import Link from "next/link";
import type { FighterBoutListResponse } from "@/lib/types/fighter";
import { outcomeBadge } from "@/components/ui/Badge";
import { Badge } from "@/components/ui/Badge";
import { Pagination } from "@/components/ui/Pagination";

interface Props {
  bouts: FighterBoutListResponse;
  fighterId: string;
}

export function FighterBoutHistory({ bouts, fighterId }: Props) {
  return (
    <div>
      <h2 className="text-lg font-semibold text-zinc-200 mb-3">Fight History</h2>
      {bouts.data.length === 0 ? (
        <p className="text-zinc-500 text-sm">No bouts recorded.</p>
      ) : (
        <>
          <div className="space-y-2">
            {bouts.data.map((bout) => {
              const self = bout.participants.find((p) => p.fighter_id === fighterId);
              const opponent = bout.participants.find(
                (p) => p.fighter_id !== fighterId,
              );

              return (
                <Link
                  key={bout.bout_id}
                  href={`/bouts/${bout.bout_id}`}
                  className="block bg-zinc-900 border border-zinc-800 rounded-lg p-4 hover:border-zinc-700 transition-colors cursor-pointer"
                >
                  <div className="flex items-center justify-between gap-4">
                    <div className="flex items-center gap-3 min-w-0">
                      {self && outcomeBadge(self.outcome_key)}
                      <div className="min-w-0">
                        <p className="text-sm text-zinc-100 truncate">
                          vs.{" "}
                          <span className="font-medium">
                            {opponent?.display_name ?? "Unknown"}
                          </span>
                        </p>
                        <p className="text-xs text-zinc-500 truncate">
                          {bout.event_name}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3 shrink-0 text-right">
                      <div>
                        <p className="text-xs text-zinc-400">
                          {bout.method_group ?? ""}
                          {bout.decision_type ? ` (${bout.decision_type})` : ""}
                        </p>
                        {bout.finish_round && (
                          <p className="text-xs text-zinc-500">R{bout.finish_round}</p>
                        )}
                      </div>
                      <div className="flex items-center gap-2">
                        {bout.is_title_fight && <Badge variant="title">Title</Badge>}
                        <span className="text-xs font-data text-zinc-500">
                          {bout.event_date}
                        </span>
                      </div>
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
          <Pagination pagination={bouts.pagination} paramName="bout_offset" />
        </>
      )}
    </div>
  );
}
