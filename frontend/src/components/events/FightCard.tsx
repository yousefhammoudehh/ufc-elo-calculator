import Link from "next/link";
import type { BoutCard } from "@/lib/types/event";
import { outcomeBadge, Badge } from "@/components/ui/Badge";

interface Props {
  bout: BoutCard;
}

export function FightCard({ bout }: Props) {
  const red = bout.participants.find((p) => p.corner === "red");
  const blue = bout.participants.find((p) => p.corner === "blue");

  return (
    <Link
      href={`/bouts/${bout.bout_id}`}
      className="block bg-zinc-900 border border-zinc-800 rounded-lg p-4 hover:border-zinc-700 transition-colors cursor-pointer"
    >
      <div className="flex items-center justify-between gap-4">
        <div className="flex items-center gap-4 min-w-0 flex-1">
          {/* Red corner */}
          <div className="flex items-center gap-2 min-w-0 flex-1">
            {red && outcomeBadge(red.outcome_key)}
            <span className="text-sm text-zinc-100 truncate">
              {red?.display_name ?? "TBD"}
            </span>
          </div>
          <span className="text-xs text-zinc-600 shrink-0">vs</span>
          {/* Blue corner */}
          <div className="flex items-center gap-2 min-w-0 flex-1 justify-end">
            <span className="text-sm text-zinc-100 truncate text-right">
              {blue?.display_name ?? "TBD"}
            </span>
            {blue && outcomeBadge(blue.outcome_key)}
          </div>
        </div>
        <div className="shrink-0 flex items-center gap-3 text-right">
          {bout.is_title_fight && <Badge variant="title">Title</Badge>}
          <div>
            <p className="text-xs text-zinc-400">
              {bout.weight_class_raw ?? bout.division_key ?? ""}
            </p>
            <p className="text-xs text-zinc-500">
              {bout.method_group ?? ""}
              {bout.decision_type ? ` (${bout.decision_type})` : ""}
              {bout.finish_round ? ` R${bout.finish_round}` : ""}
            </p>
          </div>
        </div>
      </div>
    </Link>
  );
}
