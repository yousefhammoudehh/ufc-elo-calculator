export interface BoutParticipantResponse {
  fighter_id: string;
  display_name: string;
  corner: string;
  outcome_key: string;
}

export interface FightStatsResponse {
  fighter_id: string;
  kd: number;
  sig_landed: number;
  sig_attempted: number;
  total_landed: number;
  total_attempted: number;
  td_landed: number;
  td_attempted: number;
  sub_attempts: number;
  ctrl_seconds: number;
}

export interface RatingChangeResponse {
  fighter_id: string;
  system_key: string;
  pre_rating: number;
  post_rating: number;
  delta_rating: number;
  expected_win_prob: number | null;
}

export interface PerformanceScoreResponse {
  fighter_id: string;
  ps_fight: number;
  quality_of_win: number;
}

export interface BoutDetailResponse {
  bout_id: string;
  event_id: string;
  event_date: string;
  event_name: string;
  sport_key: string;
  division_key: string | null;
  weight_class_raw: string | null;
  is_title_fight: boolean;
  method_group: string | null;
  decision_type: string | null;
  finish_round: number | null;
  finish_time_seconds: number | null;
  scheduled_rounds: number | null;
  referee: string | null;
  participants: BoutParticipantResponse[];
  fight_stats: FightStatsResponse[];
  rating_changes: RatingChangeResponse[];
  performance_scores: PerformanceScoreResponse[];
}
