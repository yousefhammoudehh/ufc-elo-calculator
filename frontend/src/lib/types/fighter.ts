import type { PaginationMeta } from "./api";

export interface FighterSummary {
  fighter_id: string;
  display_name: string;
  nickname: string | null;
  country_code: string;
  sex: string;
}

export interface FighterListResponse {
  data: FighterSummary[];
  pagination: PaginationMeta;
}

export interface FighterRating {
  system_key: string;
  rating_mean: number;
  rd: number | null;
  peak_rating: number;
}

export interface FighterProfileResponse {
  fighter_id: string;
  display_name: string;
  nickname: string | null;
  birth_date: string | null;
  birth_place: string | null;
  country_code: string;
  fighting_out_of: string | null;
  affiliation_gym: string | null;
  foundation_style: string | null;
  profile_image_url: string | null;
  height_cm: number | null;
  reach_cm: number | null;
  stance: string | null;
  sex: string;
  ratings: FighterRating[];
}

export interface TimeseriesPoint {
  date: string;
  rating_mean: number;
  rd: number | null;
}

export interface FighterTimeseriesResponse {
  fighter_id: string;
  system_key: string;
  data: TimeseriesPoint[];
  pagination: PaginationMeta;
}

export interface BoutParticipantSummary {
  fighter_id: string;
  display_name: string;
  corner: string;
  outcome_key: string;
}

export interface FighterBoutSummary {
  bout_id: string;
  event_id: string;
  event_date: string;
  event_name: string;
  division_key: string | null;
  weight_class_raw: string | null;
  is_title_fight: boolean;
  method_group: string | null;
  decision_type: string | null;
  finish_round: number | null;
  finish_time_seconds: number | null;
  participants: BoutParticipantSummary[];
}

export interface FighterBoutListResponse {
  fighter_id: string;
  data: FighterBoutSummary[];
  pagination: PaginationMeta;
}
