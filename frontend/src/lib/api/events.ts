import { apiFetch } from "./client";
import type { EventListResponse, EventDetailResponse } from "@/lib/types/event";

export function listEvents(
  limit = 25,
  offset = 0,
): Promise<EventListResponse> {
  return apiFetch("/api/v1/events", { limit, offset }, { cache: "no-store" });
}

export function getEventDetail(eventId: string): Promise<EventDetailResponse> {
  return apiFetch(`/api/v1/events/${eventId}`, undefined, {
    next: { revalidate: 300 },
  });
}
