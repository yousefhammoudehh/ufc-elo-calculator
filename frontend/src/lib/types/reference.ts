export interface DivisionResponse {
  division_id: string;
  division_key: string;
  display_name: string | null;
  sex: string;
  limit_lbs: number | null;
  is_canonical_mma: boolean;
}

export interface DivisionListResponse {
  data: DivisionResponse[];
}

export interface RatingSystemResponse {
  system_id: string;
  system_key: string;
  description: string | null;
}

export interface SystemListResponse {
  data: RatingSystemResponse[];
}
