import type { PaginationMeta } from "./api";

export interface RankingEntry {
  rank: number;
  fighter_id: string;
  display_name: string;
  rating_mean: number;
  rd: number | null;
  last_fight_date: string | null;
}

export interface RankingListResponse {
  system_key: string;
  division_key: string;
  sex: string;
  as_of_date: string;
  data: RankingEntry[];
  pagination: PaginationMeta;
}
