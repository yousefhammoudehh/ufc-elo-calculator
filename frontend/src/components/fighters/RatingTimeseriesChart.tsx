"use client";

import {
  ComposedChart,
  Area,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import type { TimeseriesPoint } from "@/lib/types/fighter";

interface Props {
  data: TimeseriesPoint[];
  systemKey: string;
}

interface ChartDatum {
  date: string;
  rating: number;
  band: [number, number];
}

export function RatingTimeseriesChart({ data, systemKey }: Props) {
  if (data.length === 0) {
    return (
      <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-8 text-center text-zinc-500 text-sm">
        No timeseries data available for {systemKey}.
      </div>
    );
  }

  const chartData: ChartDatum[] = data.map((pt) => ({
    date: pt.date,
    rating: pt.rating_mean,
    band:
      pt.rd != null
        ? [pt.rating_mean - pt.rd, pt.rating_mean + pt.rd]
        : [pt.rating_mean, pt.rating_mean],
  }));

  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-lg p-4">
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart
          data={chartData}
          margin={{ top: 8, right: 16, bottom: 8, left: 0 }}
        >
          <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
          <XAxis
            dataKey="date"
            tick={{ fill: "#71717a", fontSize: 11 }}
            tickFormatter={(d: string) => d.slice(0, 7)}
          />
          <YAxis
            tick={{ fill: "#71717a", fontSize: 11 }}
            domain={["auto", "auto"]}
          />
          <Tooltip
            contentStyle={{
              background: "#18181b",
              border: "1px solid #3f3f46",
              borderRadius: 6,
            }}
            labelStyle={{ color: "#a1a1aa" }}
            itemStyle={{ color: "#fbbf24" }}
            formatter={(value: number | [number, number], name: string) => {
              if (name === "band") return null;
              return [(value as number).toFixed(1), "Rating"];
            }}
          />
          <Area
            type="monotone"
            dataKey="band"
            fill="#fbbf24"
            fillOpacity={0.08}
            stroke="none"
          />
          <Line
            type="monotone"
            dataKey="rating"
            stroke="#fbbf24"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 4, fill: "#fbbf24" }}
          />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}
