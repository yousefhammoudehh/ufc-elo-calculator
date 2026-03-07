"use client";

import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { useDebounce } from "@/hooks/useDebounce";
import { Spinner } from "@/components/ui/Spinner";
import type { FighterSummary } from "@/lib/types/fighter";

export function FighterSearch() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<FighterSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [open, setOpen] = useState(false);
  const debouncedQuery = useDebounce(query, 300);
  const router = useRouter();
  const wrapperRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!debouncedQuery.trim()) {
      setResults([]);
      setOpen(false);
      return;
    }
    setLoading(true);
    fetch(`/api/v1/fighters?q=${encodeURIComponent(debouncedQuery)}&limit=8`)
      .then((r) => {
        if (!r.ok) throw new Error(`API error ${r.status}`);
        return r.json();
      })
      .then((data) => {
        setResults(data.data ?? []);
        setOpen(true);
      })
      .catch(() => {
        setResults([]);
        setOpen(false);
      })
      .finally(() => setLoading(false));
  }, [debouncedQuery]);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function select(fighterId: string) {
    setOpen(false);
    setQuery("");
    router.push(`/fighters/${fighterId}`);
  }

  return (
    <div ref={wrapperRef} className="relative w-full max-w-sm">
      <div className="relative">
        <svg
          className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          strokeWidth={2}
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z"
          />
        </svg>
        <input
          type="search"
          placeholder="Search fighters..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onFocus={() => results.length > 0 && setOpen(true)}
          className="w-full bg-zinc-900 border border-zinc-700 rounded-lg pl-10 pr-10 py-2 text-sm text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-1 focus:ring-amber-500 transition-shadow"
        />
        {loading && (
          <div className="absolute right-3 top-1/2 -translate-y-1/2">
            <Spinner className="h-4 w-4" />
          </div>
        )}
      </div>
      {open && results.length > 0 && (
        <ul className="absolute z-50 mt-1 w-full bg-zinc-900 border border-zinc-700 rounded-lg overflow-hidden shadow-2xl">
          {results.map((f) => (
            <li key={f.fighter_id}>
              <button
                className="w-full px-4 py-2.5 text-left hover:bg-zinc-800 text-sm flex justify-between items-center cursor-pointer transition-colors"
                onClick={() => select(f.fighter_id)}
              >
                <span className="text-zinc-100">
                  {f.display_name}
                  {f.nickname && (
                    <span className="text-zinc-500 ml-2">&quot;{f.nickname}&quot;</span>
                  )}
                </span>
                <span className="text-zinc-600 text-xs">{f.country_code}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
