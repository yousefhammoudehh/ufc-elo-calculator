"""Division-relative percentile pools with shrinkage.

Maintains running distributions of each raw UFC 5 feature per division.
Only fighters with a minimum number of tier-A fights are included in the
pools.  Provides shrunk percentile lookups used by the target-score
generator in System H.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from dataclasses import dataclass, field

from elo_calculator.application.ranking.types import UFC5BoutFeatures

_EPS = 1e-9


# ---------------------------------------------------------------------------
# Feature keys — each feature that goes into the percentile pool
# ---------------------------------------------------------------------------

# We key them as plain strings rather than an enum to keep this module
# lightweight.  The system_h module maps stat-key → feature-keys.

ALL_FEATURE_KEYS: tuple[str, ...] = (
    'sig_acc',
    'sig_def',
    'head_evasion',
    'slpm',
    'sapm',
    'str_diff',
    'td15',
    'td_acc',
    'td_def',
    'sub15',
    'opp_sub15',
    'ctrl_share',
    'opp_ctrl_share',
    'rev15',
    'opp_rev15',
    'head_share',
    'body_share',
    'leg_share',
    'dist_share',
    'clinch_share',
    'ground_share',
    'head_sig_per_min',
    'body_sig_per_min',
    'leg_sig_per_min',
    'dist_sig_per_min',
    'clinch_sig_per_min',
    'ground_sig_per_min',
    'kd15',
    'kd_abs15',
    'head_abs15',
    'body_abs15',
    'leg_abs15',
    'late_output_ratio',
    'late_abs_ratio',
    'early_output_ratio',
    'ko_win_rate',
    'sub_win_rate',
    'total_fight_minutes',
    'kd_survival_rate',
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PercentilePoolConfig:
    """Knobs for the percentile pool."""

    min_tier_a_fights: int = 5
    # Global fallback division used when a fighter's division is unknown or
    # when a specific division has fewer than `min_pool_size` observations.
    global_pool_key: str = '__global__'
    min_pool_size: int = 15


DEFAULT_POOL_CONFIG = PercentilePoolConfig()


# ---------------------------------------------------------------------------
# Shrinkage helper
# ---------------------------------------------------------------------------


def shrink(value: float, sample_size: float, tau: float, prior_mean: float) -> float:
    """Bayesian shrinkage toward the division mean.

    ``shrunk = (n / (n + tau)) * x + (tau / (n + tau)) * mean``
    """
    n = max(sample_size, 0.0)
    weight = n / (n + tau)
    return weight * value + (1.0 - weight) * prior_mean


# ---------------------------------------------------------------------------
# Pool
# ---------------------------------------------------------------------------


@dataclass
class DivisionPercentilePool:
    """Running per-division distributions for feature percentile lookups."""

    config: PercentilePoolConfig = field(default_factory=lambda: DEFAULT_POOL_CONFIG)

    # division_key → feature_key → sorted list of values
    _pools: dict[str, dict[str, list[float]]] = field(default_factory=lambda: defaultdict(lambda: defaultdict(list)))
    # division_key → count of observations (fighters)
    _counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    # ------------------------------------------------------------------
    # Insert
    # ------------------------------------------------------------------

    def add_fighter_features(self, division_id: str | None, features: UFC5BoutFeatures, tier_a_count: int) -> None:
        """Register a fighter's career-average features in the pools.

        Only added if the fighter has at least ``min_tier_a_fights`` tier-A
        bouts.
        """
        if tier_a_count < self.config.min_tier_a_fights:
            return

        div_key = division_id or self.config.global_pool_key
        self._insert_features(div_key, features)
        # Always also feed the global pool so it can serve as fallback
        if div_key != self.config.global_pool_key:
            self._insert_features(self.config.global_pool_key, features)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def percentile(self, division_id: str | None, feature_key: str, value: float) -> float:
        """Return the percentile [0.0, 1.0] of *value* within the division pool.

        Falls back to the global pool when the division pool is too small.
        """
        div_key = division_id or self.config.global_pool_key
        pool = self._pools.get(div_key, {}).get(feature_key)
        if pool is None or len(pool) < self.config.min_pool_size:
            pool = self._pools.get(self.config.global_pool_key, {}).get(feature_key)
        if pool is None or len(pool) == 0:
            return 0.5  # no information → middle
        idx = bisect.bisect_left(pool, value)
        return idx / len(pool)

    def division_mean(self, division_id: str | None, feature_key: str) -> float:
        """Return the arithmetic mean of a feature within the division pool."""
        div_key = division_id or self.config.global_pool_key
        pool = self._pools.get(div_key, {}).get(feature_key)
        if pool is None or len(pool) < self.config.min_pool_size:
            pool = self._pools.get(self.config.global_pool_key, {}).get(feature_key)
        if pool is None or len(pool) == 0:
            return 0.0
        return sum(pool) / len(pool)

    def pool_size(self, division_id: str | None) -> int:
        div_key = division_id or self.config.global_pool_key
        return self._counts.get(div_key, 0)

    # ------------------------------------------------------------------
    # Inverted percentile helper
    # ------------------------------------------------------------------

    def inv_percentile(self, division_id: str | None, feature_key: str, value: float) -> float:
        """Percentile where *lower values are better* (e.g. damage absorbed).

        Returns ``1.0 - percentile(...)``."""
        return 1.0 - self.percentile(division_id, feature_key, value)

    # ------------------------------------------------------------------
    # Clear / reset
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Reset all pools."""
        self._pools.clear()
        self._counts.clear()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _insert_features(self, div_key: str, features: UFC5BoutFeatures) -> None:
        pool = self._pools[div_key]
        for fk in ALL_FEATURE_KEYS:
            val = getattr(features, fk, None)
            if val is None:
                continue
            bisect.insort(pool[fk], float(val))
        self._counts[div_key] = self._counts.get(div_key, 0) + 1
