import type { FightStatsResponse, BoutParticipantResponse } from "@/lib/types/bout";

interface Props {
  stats: FightStatsResponse[];
  participants: BoutParticipantResponse[];
}

function formatCtrl(seconds: number): string {
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}:${String(s).padStart(2, "0")}`;
}

interface StatRow {
  label: string;
  redValue: string;
  blueValue: string;
  redRaw: number;
  blueRaw: number;
}

export function FightStatsTable({ stats, participants }: Props) {
  if (stats.length < 2) return null;

  const red = participants.find((p) => p.corner === "red");
  const blue = participants.find((p) => p.corner === "blue");
  const redStats = stats.find((s) => s.fighter_id === red?.fighter_id);
  const blueStats = stats.find((s) => s.fighter_id === blue?.fighter_id);

  if (!redStats || !blueStats) return null;

  const rows: StatRow[] = [
    {
      label: "Knockdowns",
      redValue: String(redStats.kd),
      blueValue: String(blueStats.kd),
      redRaw: redStats.kd,
      blueRaw: blueStats.kd,
    },
    {
      label: "Sig. Strikes",
      redValue: `${redStats.sig_landed}/${redStats.sig_attempted}`,
      blueValue: `${blueStats.sig_landed}/${blueStats.sig_attempted}`,
      redRaw: redStats.sig_landed,
      blueRaw: blueStats.sig_landed,
    },
    {
      label: "Total Strikes",
      redValue: `${redStats.total_landed}/${redStats.total_attempted}`,
      blueValue: `${blueStats.total_landed}/${blueStats.total_attempted}`,
      redRaw: redStats.total_landed,
      blueRaw: blueStats.total_landed,
    },
    {
      label: "Takedowns",
      redValue: `${redStats.td_landed}/${redStats.td_attempted}`,
      blueValue: `${blueStats.td_landed}/${blueStats.td_attempted}`,
      redRaw: redStats.td_landed,
      blueRaw: blueStats.td_landed,
    },
    {
      label: "Sub. Attempts",
      redValue: String(redStats.sub_attempts),
      blueValue: String(blueStats.sub_attempts),
      redRaw: redStats.sub_attempts,
      blueRaw: blueStats.sub_attempts,
    },
    {
      label: "Control Time",
      redValue: formatCtrl(redStats.ctrl_seconds),
      blueValue: formatCtrl(blueStats.ctrl_seconds),
      redRaw: redStats.ctrl_seconds,
      blueRaw: blueStats.ctrl_seconds,
    },
  ];

  return (
    <div>
      <h2 className="text-lg font-semibold text-zinc-200 mb-3">Fight Statistics</h2>
      <div className="bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden">
        <div className="grid grid-cols-3 px-4 py-2 border-b border-zinc-800 text-xs text-zinc-500 uppercase tracking-wider">
          <span className="text-left">{red?.display_name ?? "Red"}</span>
          <span className="text-center">Stat</span>
          <span className="text-right">{blue?.display_name ?? "Blue"}</span>
        </div>
        {rows.map((row) => {
          const redWins = row.redRaw > row.blueRaw;
          const blueWins = row.blueRaw > row.redRaw;

          return (
            <div
              key={row.label}
              className="grid grid-cols-3 px-4 py-3 border-b border-zinc-800/50 last:border-0"
            >
              <span
                className={`text-left font-data text-sm ${redWins ? "text-amber-400" : "text-zinc-400"}`}
              >
                {row.redValue}
              </span>
              <span className="text-center text-xs text-zinc-500">{row.label}</span>
              <span
                className={`text-right font-data text-sm ${blueWins ? "text-amber-400" : "text-zinc-400"}`}
              >
                {row.blueValue}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
