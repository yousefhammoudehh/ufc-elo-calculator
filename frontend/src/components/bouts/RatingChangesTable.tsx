import type {
  RatingChangeResponse,
  PerformanceScoreResponse,
  BoutParticipantResponse,
} from "@/lib/types/bout";

interface Props {
  ratingChanges: RatingChangeResponse[];
  performanceScores: PerformanceScoreResponse[];
  participants: BoutParticipantResponse[];
}

function formatSystemKey(key: string): string {
  return key.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

export function RatingChangesTable({
  ratingChanges,
  performanceScores,
  participants,
}: Props) {
  if (ratingChanges.length === 0) return null;

  const red = participants.find((p) => p.corner === "red");
  const blue = participants.find((p) => p.corner === "blue");

  // Group rating changes by system
  const systems = [...new Set(ratingChanges.map((r) => r.system_key))];

  return (
    <div className="space-y-6">
      {/* Rating Changes */}
      <div>
        <h2 className="text-lg font-semibold text-zinc-200 mb-3">Rating Changes</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left">
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                  System
                </th>
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium">
                  Fighter
                </th>
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                  Pre
                </th>
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                  Post
                </th>
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                  Delta
                </th>
                <th className="py-2 px-3 text-xs text-zinc-500 uppercase tracking-wider font-medium text-right">
                  Win Prob
                </th>
              </tr>
            </thead>
            <tbody>
              {systems.map((systemKey) => {
                const changes = ratingChanges.filter(
                  (r) => r.system_key === systemKey,
                );
                return changes.map((change, i) => {
                  const fighter = participants.find(
                    (p) => p.fighter_id === change.fighter_id,
                  );
                  return (
                    <tr
                      key={`${systemKey}-${change.fighter_id}`}
                      className="border-b border-zinc-800/50 hover:bg-zinc-800/30 transition-colors"
                    >
                      {i === 0 && (
                        <td
                          rowSpan={changes.length}
                          className="py-2 px-3 text-zinc-400 text-xs align-top"
                        >
                          {formatSystemKey(systemKey)}
                        </td>
                      )}
                      <td className="py-2 px-3 text-zinc-200">
                        {fighter?.display_name ?? change.fighter_id}
                      </td>
                      <td className="py-2 px-3 text-right font-data text-zinc-400">
                        {change.pre_rating.toFixed(1)}
                      </td>
                      <td className="py-2 px-3 text-right font-data text-zinc-200">
                        {change.post_rating.toFixed(1)}
                      </td>
                      <td
                        className={`py-2 px-3 text-right font-data ${change.delta_rating > 0 ? "text-green-400" : change.delta_rating < 0 ? "text-red-400" : "text-zinc-400"}`}
                      >
                        {change.delta_rating > 0 ? "+" : ""}
                        {change.delta_rating.toFixed(1)}
                      </td>
                      <td className="py-2 px-3 text-right font-data text-zinc-500">
                        {change.expected_win_prob != null
                          ? `${(change.expected_win_prob * 100).toFixed(1)}%`
                          : "\u2014"}
                      </td>
                    </tr>
                  );
                });
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Performance Scores */}
      {performanceScores.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-zinc-200 mb-3">
            Performance Scores
          </h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {performanceScores.map((ps) => {
              const fighter =
                participants.find((p) => p.fighter_id === ps.fighter_id) ?? null;
              const isRed = fighter?.fighter_id === red?.fighter_id;

              return (
                <div
                  key={ps.fighter_id}
                  className="bg-zinc-900 border border-zinc-800 rounded-lg p-4"
                >
                  <p className="text-sm text-zinc-400 mb-2">
                    {fighter?.display_name ?? "Unknown"}
                    {isRed ? " (Red)" : " (Blue)"}
                  </p>
                  <div className="flex gap-6">
                    <div>
                      <p className="text-xs text-zinc-500 uppercase">PS Fight</p>
                      <p className="text-xl font-data text-amber-400">
                        {ps.ps_fight.toFixed(3)}
                      </p>
                    </div>
                    <div>
                      <p className="text-xs text-zinc-500 uppercase">
                        Quality of Win
                      </p>
                      <p className="text-xl font-data text-zinc-200">
                        {ps.quality_of_win.toFixed(3)}
                      </p>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
