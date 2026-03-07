import type { Metadata } from "next";
import { listEvents } from "@/lib/api/events";
import { PageShell } from "@/components/layout/PageShell";
import { EventCard } from "@/components/events/EventCard";
import { Pagination } from "@/components/ui/Pagination";

export const metadata: Metadata = { title: "Events" };

interface Props {
  searchParams: Promise<{ offset?: string }>;
}

export default async function EventsPage({ searchParams }: Props) {
  const params = await searchParams;
  const offset = Number(params.offset ?? 0);

  const data = await listEvents(25, offset);

  return (
    <PageShell title="Events" subtitle="All UFC events, most recent first">
      <div className="space-y-2">
        {data.data.map((event) => (
          <EventCard key={event.event_id} event={event} />
        ))}
      </div>
      <Pagination pagination={data.pagination} />
    </PageShell>
  );
}
