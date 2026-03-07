import type { PaginationMeta } from "./api";

export interface EventSummary {
  event_id: string;
  event_date: string;
  event_name: string;
  promotion_name: string | null;
  location: string | null;
  num_fights: number | null;
}

export interface EventListResponse {
  data: EventSummary[];
  pagination: PaginationMeta;
}

export interface ParticipantCard {
  fighter_id: string;
  display_name: string;
  corner: string;
  outcome_key: string;
}

export interface BoutCard {
  bout_id: string;
  division_key: string | null;
  weight_class_raw: string | null;
  is_title_fight: boolean;
  method_group: string | null;
  decision_type: string | null;
  finish_round: number | null;
  finish_time_seconds: number | null;
  participants: ParticipantCard[];
}

export interface EventDetailResponse {
  event_id: string;
  event_date: string;
  event_name: string;
  promotion_name: string | null;
  location: string | null;
  bouts: BoutCard[];
}
