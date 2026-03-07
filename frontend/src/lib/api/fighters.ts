import { apiFetch } from "./client";
import type {
  FighterListResponse,
  FighterProfileResponse,
  FighterBoutListResponse,
  FighterTimeseriesResponse,
} from "@/lib/types/fighter";

export function searchFighters(
  q: string,
  limit = 25,
  offset = 0,
): Promise<FighterListResponse> {
  return apiFetch("/api/v1/fighters", { q, limit, offset }, { cache: "no-store" });
}

export function getFighterProfile(
  fighterId: string,
): Promise<FighterProfileResponse> {
  return apiFetch(`/api/v1/fighters/${fighterId}`, undefined, {
    next: { revalidate: 300 },
  });
}

export function getFighterBouts(
  fighterId: string,
  limit = 25,
  offset = 0,
): Promise<FighterBoutListResponse> {
  return apiFetch(`/api/v1/fighters/${fighterId}/bouts`, { limit, offset }, {
    next: { revalidate: 300 },
  });
}

export function getFighterTimeseries(
  fighterId: string,
  system = "unified_composite_elo",
  limit = 500,
): Promise<FighterTimeseriesResponse> {
  return apiFetch(
    `/api/v1/fighters/${fighterId}/timeseries`,
    { system, limit },
    { next: { revalidate: 300 } },
  );
}
