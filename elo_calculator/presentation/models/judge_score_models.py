from elo_calculator.presentation.models.shared import DataModel


class JudgeScoreCreateRequest(DataModel):
    bout_id: str
    fighter_id: str
    judge1_score: int | None = None
    judge2_score: int | None = None
    judge3_score: int | None = None


class JudgeScoreUpdateRequest(DataModel):
    judge1_score: int | None = None
    judge2_score: int | None = None
    judge3_score: int | None = None


class JudgeScoreResponse(DataModel):
    bout_id: str
    fighter_id: str
    judge1_score: int | None = None
    judge2_score: int | None = None
    judge3_score: int | None = None
