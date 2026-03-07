import { apiFetch } from "./client";
import type { RankingListResponse } from "@/lib/types/ranking";

export function getRankings(
  system: string,
  division: string,
  sex: string,
  limit = 50,
  offset = 0,
): Promise<RankingListResponse> {
  return apiFetch(
    "/api/v1/rankings",
    { system, division, sex, limit, offset },
    { cache: "no-store" },
  );
}
