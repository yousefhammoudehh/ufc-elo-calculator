import { apiFetch } from "./client";
import type { BoutDetailResponse } from "@/lib/types/bout";

export function getBoutDetail(boutId: string): Promise<BoutDetailResponse> {
  return apiFetch(`/api/v1/bouts/${boutId}`, undefined, {
    next: { revalidate: 300 },
  });
}
