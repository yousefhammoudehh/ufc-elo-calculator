import { apiFetch } from "./client";
import type { DivisionListResponse, SystemListResponse } from "@/lib/types/reference";

export function getDivisions(): Promise<DivisionListResponse> {
  return apiFetch("/api/v1/divisions", undefined, {
    next: { revalidate: 3600 },
  });
}

export function getSystems(): Promise<SystemListResponse> {
  return apiFetch("/api/v1/systems", undefined, {
    next: { revalidate: 3600 },
  });
}
