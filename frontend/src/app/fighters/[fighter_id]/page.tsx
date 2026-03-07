import type { Metadata } from "next";
import { notFound } from "next/navigation";
import {
  getFighterProfile,
  getFighterBouts,
  getFighterTimeseries,
} from "@/lib/api/fighters";
import { ApiError } from "@/lib/api/client";
import { FighterHero } from "@/components/fighters/FighterHero";
import { FighterRatingsGrid } from "@/components/fighters/FighterRatingsGrid";
import { RatingTimeseriesChart } from "@/components/fighters/RatingTimeseriesChart";
import { FighterBoutHistory } from "@/components/fighters/FighterBoutHistory";

interface Props {
  params: Promise<{ fighter_id: string }>;
  searchParams: Promise<{ system?: string; bout_offset?: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { fighter_id } = await params;
  try {
    const profile = await getFighterProfile(fighter_id);
    return { title: profile.display_name };
  } catch {
    return { title: "Fighter" };
  }
}

export default async function FighterProfilePage({
  params,
  searchParams,
}: Props) {
  const { fighter_id } = await params;
  const sp = await searchParams;
  const system = sp.system ?? "unified_composite_elo";
  const boutOffset = Number(sp.bout_offset ?? 0);

  let profile;
  try {
    profile = await getFighterProfile(fighter_id);
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) notFound();
    throw e;
  }

  const [boutsData, timeseriesData] = await Promise.all([
    getFighterBouts(fighter_id, 10, boutOffset),
    getFighterTimeseries(fighter_id, system).catch(() => null),
  ]);

  return (
    <div className="space-y-8">
      <FighterHero profile={profile} />
      <FighterRatingsGrid ratings={profile.ratings} />
      {timeseriesData && (
        <section>
          <h2 className="text-lg font-semibold text-zinc-200 mb-3">
            Rating History
            <span className="text-sm font-normal text-zinc-500 ml-2">
              {system.replace(/_/g, " ")}
            </span>
          </h2>
          <RatingTimeseriesChart data={timeseriesData.data} systemKey={system} />
        </section>
      )}
      <FighterBoutHistory bouts={boutsData} fighterId={fighter_id} />
    </div>
  );
}
