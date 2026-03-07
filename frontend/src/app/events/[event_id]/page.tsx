import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getEventDetail } from "@/lib/api/events";
import { ApiError } from "@/lib/api/client";
import { PageShell } from "@/components/layout/PageShell";
import { FightCard } from "@/components/events/FightCard";

interface Props {
  params: Promise<{ event_id: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { event_id } = await params;
  try {
    const event = await getEventDetail(event_id);
    return { title: event.event_name };
  } catch {
    return { title: "Event" };
  }
}

export default async function EventDetailPage({ params }: Props) {
  const { event_id } = await params;

  let event;
  try {
    event = await getEventDetail(event_id);
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) notFound();
    throw e;
  }

  return (
    <PageShell
      title={event.event_name}
      subtitle={[event.event_date, event.promotion_name, event.location]
        .filter(Boolean)
        .join(" \u00b7 ")}
    >
      <div>
        <h2 className="text-lg font-semibold text-zinc-200 mb-3">
          Fight Card
          <span className="text-sm font-normal text-zinc-500 ml-2">
            {event.bouts.length} bout{event.bouts.length !== 1 ? "s" : ""}
          </span>
        </h2>
        <div className="space-y-2">
          {event.bouts.map((bout) => (
            <FightCard key={bout.bout_id} bout={bout} />
          ))}
        </div>
      </div>
    </PageShell>
  );
}
