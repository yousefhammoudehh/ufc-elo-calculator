import type { Metadata } from "next";
import { searchFighters } from "@/lib/api/fighters";
import { PageShell } from "@/components/layout/PageShell";
import { FighterSearch } from "@/components/fighters/FighterSearch";
import { EmptyState } from "@/components/ui/EmptyState";
import { Pagination } from "@/components/ui/Pagination";
import Link from "next/link";

export const metadata: Metadata = { title: "Fighters" };

interface Props {
  searchParams: Promise<{ q?: string; offset?: string }>;
}

export default async function FightersPage({ searchParams }: Props) {
  const params = await searchParams;
  const q = params.q ?? "";
  const offset = Number(params.offset ?? 0);

  const data = await searchFighters(q, 25, offset).catch(() => null);

  return (
    <PageShell title="Fighters" subtitle="Search and browse all fighters">
      <div className="max-w-lg">
        <FighterSearch />
      </div>
      {data && data.data.length > 0 ? (
        <div className="mt-6">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-zinc-800 text-left">
                  <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                    Name
                  </th>
                  <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                    Nickname
                  </th>
                  <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                    Country
                  </th>
                  <th className="py-3 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                    Sex
                  </th>
                </tr>
              </thead>
              <tbody>
                {data.data.map((f) => (
                  <tr
                    key={f.fighter_id}
                    className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors"
                  >
                    <td className="py-3 px-3">
                      <Link
                        href={`/fighters/${f.fighter_id}`}
                        className="text-zinc-100 hover:text-amber-400 transition-colors"
                      >
                        {f.display_name}
                      </Link>
                    </td>
                    <td className="py-3 px-3 text-zinc-400">
                      {f.nickname ?? "\u2014"}
                    </td>
                    <td className="py-3 px-3 text-zinc-400 font-data">{f.country_code}</td>
                    <td className="py-3 px-3 text-zinc-400">{f.sex}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination pagination={data.pagination} />
        </div>
      ) : (
        <EmptyState message={data ? "No fighters found." : "Failed to load fighters."} />
      )}
    </PageShell>
  );
}
