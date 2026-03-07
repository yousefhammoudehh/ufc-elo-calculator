import Link from "next/link";
import type { EventSummary } from "@/lib/types/event";

interface Props {
  event: EventSummary;
}

export function EventCard({ event }: Props) {
  return (
    <Link
      href={`/events/${event.event_id}`}
      className="block bg-zinc-900 border border-zinc-800 rounded-lg p-4 hover:border-zinc-700 transition-colors cursor-pointer"
    >
      <div className="flex items-center justify-between gap-4">
        <div className="min-w-0">
          <p className="text-sm font-medium text-zinc-100 truncate">
            {event.event_name}
          </p>
          <p className="text-xs text-zinc-500 mt-1">
            {event.promotion_name && (
              <span className="text-zinc-400">{event.promotion_name} &middot; </span>
            )}
            {event.location ?? "Location TBD"}
          </p>
        </div>
        <div className="shrink-0 text-right">
          <p className="text-xs font-data text-zinc-400">{event.event_date}</p>
          {event.num_fights != null && (
            <p className="text-xs text-zinc-500 mt-1">
              {event.num_fights} fight{event.num_fights !== 1 ? "s" : ""}
            </p>
          )}
        </div>
      </div>
    </Link>
  );
}
