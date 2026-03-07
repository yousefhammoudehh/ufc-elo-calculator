import Link from "next/link";
import type { BoutDetailResponse } from "@/lib/types/bout";
import { outcomeBadge, Badge } from "@/components/ui/Badge";

interface Props {
  bout: BoutDetailResponse;
}

export function BoutHeader({ bout }: Props) {
  const red = bout.participants.find((p) => p.corner === "red");
  const blue = bout.participants.find((p) => p.corner === "blue");

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-6">
      {/* Event info */}
      <div className="text-sm text-zinc-500 mb-4">
        <Link
          href={`/events/${bout.event_id}`}
          className="hover:text-zinc-300 transition-colors"
        >
          {bout.event_name}
        </Link>
        <span> &middot; {bout.event_date}</span>
        {bout.weight_class_raw && <span> &middot; {bout.weight_class_raw}</span>}
      </div>

      {/* Fighters */}
      <div className="flex items-center justify-center gap-6 sm:gap-12">
        <div className="text-center flex-1">
          {red ? (
            <>
              <Link
                href={`/fighters/${red.fighter_id}`}
                className="text-lg font-semibold text-zinc-100 hover:text-amber-400 transition-colors"
              >
                {red.display_name}
              </Link>
              <div className="mt-2">{outcomeBadge(red.outcome_key)}</div>
            </>
          ) : (
            <p className="text-zinc-500">TBD</p>
          )}
        </div>
        <span className="text-zinc-600 text-sm font-medium shrink-0">VS</span>
        <div className="text-center flex-1">
          {blue ? (
            <>
              <Link
                href={`/fighters/${blue.fighter_id}`}
                className="text-lg font-semibold text-zinc-100 hover:text-amber-400 transition-colors"
              >
                {blue.display_name}
              </Link>
              <div className="mt-2">{outcomeBadge(blue.outcome_key)}</div>
            </>
          ) : (
            <p className="text-zinc-500">TBD</p>
          )}
        </div>
      </div>

      {/* Result info */}
      <div className="mt-4 flex items-center justify-center gap-3 flex-wrap">
        {bout.is_title_fight && <Badge variant="title">Title Fight</Badge>}
        <span className="text-sm text-zinc-400">
          {bout.method_group}
          {bout.decision_type ? ` (${bout.decision_type})` : ""}
        </span>
        {bout.finish_round && (
          <span className="text-sm text-zinc-500">
            Round {bout.finish_round}
            {bout.finish_time_seconds != null &&
              ` \u2014 ${Math.floor(bout.finish_time_seconds / 60)}:${String(bout.finish_time_seconds % 60).padStart(2, "0")}`}
          </span>
        )}
        {bout.referee && (
          <span className="text-xs text-zinc-600">Ref: {bout.referee}</span>
        )}
      </div>
    </div>
  );
}
