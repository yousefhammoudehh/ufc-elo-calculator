"use client";

import { useRouter, usePathname, useSearchParams } from "next/navigation";
import type { PaginationMeta } from "@/lib/types/api";

interface PaginationProps {
  pagination: PaginationMeta;
  paramName?: string;
}

export function Pagination({ pagination, paramName = "offset" }: PaginationProps) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const { total, limit, offset, has_next, has_previous } = pagination;
  const start = offset + 1;
  const end = Math.min(offset + limit, total);

  function navigate(newOffset: number) {
    const next = new URLSearchParams(searchParams.toString());
    next.set(paramName, String(newOffset));
    router.push(`${pathname}?${next.toString()}`);
  }

  if (total === 0) return null;

  return (
    <div className="flex items-center justify-between py-4">
      <p className="text-sm text-zinc-500">
        Showing <span className="font-data text-zinc-300">{start}-{end}</span> of{" "}
        <span className="font-data text-zinc-300">{total}</span>
      </p>
      <div className="flex gap-2">
        <button
          onClick={() => navigate(Math.max(0, offset - limit))}
          disabled={!has_previous}
          className="px-3 py-1.5 text-sm rounded bg-zinc-800 border border-zinc-700 text-zinc-300 hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
        >
          Previous
        </button>
        <button
          onClick={() => navigate(offset + limit)}
          disabled={!has_next}
          className="px-3 py-1.5 text-sm rounded bg-zinc-800 border border-zinc-700 text-zinc-300 hover:bg-zinc-700 disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors"
        >
          Next
        </button>
      </div>
    </div>
  );
}
