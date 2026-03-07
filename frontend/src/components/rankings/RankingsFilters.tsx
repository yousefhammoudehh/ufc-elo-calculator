"use client";

import { useRouter, usePathname, useSearchParams } from "next/navigation";
import { useCallback } from "react";
import type { DivisionResponse, RatingSystemResponse } from "@/lib/types/reference";

interface Props {
  divisions: DivisionResponse[];
  systems: RatingSystemResponse[];
  current: { system: string; division: string; sex: string };
}

export function RankingsFilters({ divisions, systems, current }: Props) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const update = useCallback(
    (key: string, value: string) => {
      const next = new URLSearchParams(searchParams.toString());
      next.set(key, value);
      next.delete("offset");
      router.push(`${pathname}?${next.toString()}`);
    },
    [router, pathname, searchParams],
  );

  return (
    <div className="flex flex-wrap gap-4 mb-6">
      <FilterSelect
        label="System"
        value={current.system}
        options={systems.map((s) => ({
          value: s.system_key,
          label: s.description ?? s.system_key,
        }))}
        onChange={(v) => update("system", v)}
      />
      <FilterSelect
        label="Division"
        value={current.division}
        options={divisions.map((d) => ({
          value: d.division_key,
          label: d.display_name
            ? `${d.display_name}${d.limit_lbs ? ` (${d.limit_lbs} lbs)` : ""}`
            : d.division_key,
        }))}
        onChange={(v) => update("division", v)}
      />
      <FilterSelect
        label="Sex"
        value={current.sex}
        options={[
          { value: "M", label: "Men" },
          { value: "F", label: "Women" },
        ]}
        onChange={(v) => update("sex", v)}
      />
    </div>
  );
}

function FilterSelect({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: string;
  options: { value: string; label: string }[];
  onChange: (v: string) => void;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label className="text-xs text-zinc-500 uppercase tracking-wider font-medium">
        {label}
      </label>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-zinc-900 border border-zinc-700 text-zinc-200 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-amber-500 cursor-pointer min-w-[180px]"
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </div>
  );
}
