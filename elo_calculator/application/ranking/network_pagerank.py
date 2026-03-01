from __future__ import annotations

import math
from dataclasses import dataclass

from elo_calculator.application.ranking.math_utils import logistic
from elo_calculator.application.ranking.types import BoutOutcome


@dataclass(frozen=True)
class NetworkBoutObservation:
    fighter_a_id: str
    fighter_b_id: str
    outcome_for_a: BoutOutcome
    weight: float = 1.0


@dataclass(frozen=True)
class PageRankConfig:
    damping: float = 0.85
    max_iter: int = 200
    tolerance: float = 1e-9
    logit_temperature: float = 1.0


class PageRankMarkovWinGraph:
    def __init__(self, config: PageRankConfig | None = None) -> None:
        self._config = config or PageRankConfig()
        self._scores: dict[str, float] = {}

    @property
    def scores(self) -> dict[str, float]:
        return dict(self._scores)

    def fit(self, observations: list[NetworkBoutObservation]) -> dict[str, float]:
        nodes = _collect_nodes(observations)
        if not nodes:
            self._scores = {}
            return {}

        edge_weights = _build_edge_weights(observations)
        self._scores = _power_iteration(nodes, edge_weights, self._config)
        return dict(self._scores)

    def win_probability(self, fighter_a_id: str, fighter_b_id: str) -> float:
        score_a = max(1e-12, self._scores.get(fighter_a_id, 0.0))
        score_b = max(1e-12, self._scores.get(fighter_b_id, 0.0))
        log_gap = (math.log(score_a) - math.log(score_b)) / self._config.logit_temperature
        return logistic(log_gap)

    def connectivity(self, fighter_id: str, observations: list[NetworkBoutObservation]) -> float:
        total_edges = sum(1 for obs in observations if fighter_id in {obs.fighter_a_id, obs.fighter_b_id})
        return float(total_edges)


def _collect_nodes(observations: list[NetworkBoutObservation]) -> list[str]:
    node_set = {obs.fighter_a_id for obs in observations}
    node_set.update(obs.fighter_b_id for obs in observations)
    return sorted(node_set)


def _build_edge_weights(observations: list[NetworkBoutObservation]) -> dict[tuple[str, str], float]:
    edges: dict[tuple[str, str], float] = {}
    for obs in observations:
        _update_edges(edges, obs)
    return edges


def _update_edges(edges: dict[tuple[str, str], float], observation: NetworkBoutObservation) -> None:
    if observation.outcome_for_a == BoutOutcome.WIN:
        _add_edge(edges, observation.fighter_b_id, observation.fighter_a_id, observation.weight)
        return
    if observation.outcome_for_a == BoutOutcome.LOSS:
        _add_edge(edges, observation.fighter_a_id, observation.fighter_b_id, observation.weight)
        return
    if observation.outcome_for_a == BoutOutcome.DRAW:
        split_weight = 0.5 * observation.weight
        _add_edge(edges, observation.fighter_a_id, observation.fighter_b_id, split_weight)
        _add_edge(edges, observation.fighter_b_id, observation.fighter_a_id, split_weight)


def _add_edge(edges: dict[tuple[str, str], float], loser_id: str, winner_id: str, weight: float) -> None:
    key = (loser_id, winner_id)
    edges[key] = edges.get(key, 0.0) + weight


def _power_iteration(
    nodes: list[str], edge_weights: dict[tuple[str, str], float], config: PageRankConfig
) -> dict[str, float]:
    node_count = len(nodes)
    base = (1.0 - config.damping) / node_count
    ranks = dict.fromkeys(nodes, 1.0 / node_count)
    outgoing = _outgoing_totals(nodes, edge_weights)

    for _ in range(config.max_iter):
        updated = dict.fromkeys(nodes, base)
        _distribute_rank(nodes, ranks, outgoing, edge_weights, updated, config)
        delta = sum(abs(updated[node] - ranks[node]) for node in nodes)
        ranks = updated
        if delta < config.tolerance:
            break

    return ranks


def _outgoing_totals(nodes: list[str], edge_weights: dict[tuple[str, str], float]) -> dict[str, float]:
    totals = dict.fromkeys(nodes, 0.0)
    for (loser_id, _winner_id), weight in edge_weights.items():
        totals[loser_id] += weight
    return totals


def _distribute_rank(
    nodes: list[str],
    ranks: dict[str, float],
    outgoing: dict[str, float],
    edge_weights: dict[tuple[str, str], float],
    updated: dict[str, float],
    config: PageRankConfig,
) -> None:
    node_count = len(nodes)
    dangling_sum = sum(ranks[node] for node in nodes if outgoing[node] <= 0)
    dangling_share = config.damping * dangling_sum / node_count
    for node in nodes:
        updated[node] += dangling_share

    for (loser_id, winner_id), weight in edge_weights.items():
        denominator = outgoing[loser_id]
        if denominator <= 0:
            continue
        updated[winner_id] += config.damping * ranks[loser_id] * (weight / denominator)
