from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import polars as pl


# Para WhatsApp, read implica que a mensagem foi entregue.
# Nesta base, os status estão em minúsculo.
DELIVERED_VALUES = {"delivered", "read"}


@dataclass(frozen=True)
class RateCI:
    rate: float
    n: int
    ci_low: float
    ci_high: float


def delivered_flag(col_status: pl.Expr) -> pl.Expr:
    return col_status.cast(pl.Utf8).str.to_lowercase().is_in(list(DELIVERED_VALUES)).cast(pl.Int8)


def wilson_ci(successes: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    if n <= 0:
        return (np.nan, np.nan)
    from scipy.stats import norm

    z = float(norm.ppf(1 - alpha / 2))
    phat = successes / n
    denom = 1 + z**2 / n
    center = (phat + z**2 / (2 * n)) / denom
    half = (z * np.sqrt((phat * (1 - phat) / n) + (z**2 / (4 * n**2)))) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def aggregate_rate_with_ci(
    df: pl.DataFrame,
    group_cols: Iterable[str],
    success_col: str,
) -> pl.DataFrame:
    grouped = (
        df.group_by(list(group_cols))
        .agg(
            n=pl.len(),
            successes=pl.col(success_col).sum(),
        )
        .with_columns(
            rate=(pl.col("successes") / pl.col("n")).cast(pl.Float64),
        )
    )
    rows = grouped.select(list(group_cols) + ["successes", "n", "rate"]).to_dicts()
    ci = [wilson_ci(int(r["successes"]), int(r["n"])) for r in rows]
    ci_low = [c[0] for c in ci]
    ci_high = [c[1] for c in ci]
    return grouped.with_columns(
        ci_low=pl.Series(ci_low).cast(pl.Float64),
        ci_high=pl.Series(ci_high).cast(pl.Float64),
    ).drop(["successes"])


def bootstrap_rate_diff(
    y_a: np.ndarray,
    y_b: np.ndarray,
    n_boot: int = 5000,
    seed: int = 42,
) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    y_a = y_a.astype(float)
    y_b = y_b.astype(float)
    if y_a.size == 0 or y_b.size == 0:
        return {"diff_mean": np.nan, "ci_low": np.nan, "ci_high": np.nan}
    diffs = np.empty(n_boot, dtype=float)
    for i in range(n_boot):
        sa = rng.choice(y_a, size=y_a.size, replace=True).mean()
        sb = rng.choice(y_b, size=y_b.size, replace=True).mean()
        diffs[i] = sa - sb
    return {
        "diff_mean": float(diffs.mean()),
        "ci_low": float(np.quantile(diffs, 0.025)),
        "ci_high": float(np.quantile(diffs, 0.975)),
    }

