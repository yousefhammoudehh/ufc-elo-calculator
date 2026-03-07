import type { Metadata } from "next";
import { getDivisions, getSystems } from "@/lib/api/reference";
import { getRankings } from "@/lib/api/rankings";
import { RankingsFilters } from "@/components/rankings/RankingsFilters";
import { RankingsTable } from "@/components/rankings/RankingsTable";
import { PageShell } from "@/components/layout/PageShell";
import { EmptyState } from "@/components/ui/EmptyState";

export const metadata: Metadata = { title: "Rankings" };

interface Props {
  searchParams: Promise<{
    system?: string;
    division?: string;
    sex?: string;
    offset?: string;
  }>;
}

export default async function RankingsPage({ searchParams }: Props) {
  const params = await searchParams;
  const system = params.system ?? "unified_composite_elo";
  const division = params.division ?? "MMA_LW";
  const sex = params.sex ?? "M";
  const offset = Number(params.offset ?? 0);

  const [divisionsData, systemsData, rankingsData] = await Promise.all([
    getDivisions(),
    getSystems(),
    getRankings(system, division, sex, 50, offset).catch(() => null),
  ]);

  return (
    <PageShell title="Rankings" subtitle="Fighter rankings across multiple rating systems">
      <RankingsFilters
        divisions={divisionsData.data}
        systems={systemsData.data}
        current={{ system, division, sex }}
      />
      {rankingsData && rankingsData.data.length > 0 ? (
        <RankingsTable rankings={rankingsData} />
      ) : (
        <EmptyState message="No rankings found for this combination." />
      )}
    </PageShell>
  );
}
