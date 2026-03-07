import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getBoutDetail } from "@/lib/api/bouts";
import { ApiError } from "@/lib/api/client";
import { BoutHeader } from "@/components/bouts/BoutHeader";
import { FightStatsTable } from "@/components/bouts/FightStatsTable";
import { RatingChangesTable } from "@/components/bouts/RatingChangesTable";

interface Props {
  params: Promise<{ bout_id: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { bout_id } = await params;
  try {
    const bout = await getBoutDetail(bout_id);
    const names = bout.participants.map((p) => p.display_name).join(" vs ");
    return { title: names || "Bout Detail" };
  } catch {
    return { title: "Bout Detail" };
  }
}

export default async function BoutDetailPage({ params }: Props) {
  const { bout_id } = await params;

  let bout;
  try {
    bout = await getBoutDetail(bout_id);
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) notFound();
    throw e;
  }

  return (
    <div className="space-y-8">
      <BoutHeader bout={bout} />
      <FightStatsTable stats={bout.fight_stats} participants={bout.participants} />
      <RatingChangesTable
        ratingChanges={bout.rating_changes}
        performanceScores={bout.performance_scores}
        participants={bout.participants}
      />
    </div>
  );
}
