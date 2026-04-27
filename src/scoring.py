from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl


@dataclass(frozen=True)
class SystemScoreParams:
    prior_alpha: float = 2.0
    prior_beta: float = 2.0
    half_life_days: float = 365.0
    ddd_bonus: dict[str, float] | None = None


def _beta_posterior_mean(successes: pl.Expr, n: pl.Expr, alpha0: float, beta0: float) -> pl.Expr:
    return (successes + alpha0) / (n + alpha0 + beta0)


def _recency_weight(age_days: pl.Expr, half_life_days: float) -> pl.Expr:
    ln2 = float(np.log(2.0))
    return (-ln2 * (age_days / half_life_days)).exp()


def build_system_ranking(
    df_events: pl.DataFrame,
    system_col: str,
    delivered_col: str,
    age_days_col: str,
    params: SystemScoreParams = SystemScoreParams(),
) -> pl.DataFrame:
    base = (
        df_events.group_by(system_col)
        .agg(
            n=pl.len(),
            successes=pl.col(delivered_col).sum(),
            mean_age_days=pl.col(age_days_col).mean(),
        )
        .with_columns(
            posterior_mean=_beta_posterior_mean(
                pl.col("successes"),
                pl.col("n"),
                params.prior_alpha,
                params.prior_beta,
            ).cast(pl.Float64),
        )
        .with_columns(
            score=pl.col("posterior_mean") * (pl.col("n") / (pl.col("n") + 500)).cast(pl.Float64),
        )
        .sort("score", descending=True)
    )
    return base


def score_phones_for_cpf(
    df_candidates: pl.DataFrame,
    system_rank: pl.DataFrame,
    system_col: str,
    phone_col: str,
    updated_at_col: str,
    ddd_col: str | None,
    now_ts: pl.Datetime,
    params: SystemScoreParams = SystemScoreParams(),
) -> pl.DataFrame:
    rank = system_rank.select(system_col, system_score=pl.col("score"))
    out = df_candidates.join(rank, on=system_col, how="left").with_columns(
        system_score=pl.col("system_score").fill_null(0.0),
        updated_at_dt=pl.col(updated_at_col).cast(pl.Datetime(time_unit="us")),
    ).with_columns(
        age_days=((pl.lit(now_ts) - pl.col("updated_at_dt")).dt.total_days()).cast(pl.Float64),
    )
    out = out.with_columns(
        recency_weight=_recency_weight(pl.col("age_days"), params.half_life_days).cast(pl.Float64),
    ).with_columns(
        base_score=(pl.col("system_score") * pl.col("recency_weight")).cast(pl.Float64),
    )
    if ddd_col is not None and params.ddd_bonus:
        out = out.with_columns(
            ddd_bonus=pl.col(ddd_col).cast(pl.Utf8).replace(params.ddd_bonus, default=0.0).cast(pl.Float64)
        ).with_columns(
            final_score=(pl.col("base_score") * (1.0 + pl.col("ddd_bonus"))).cast(pl.Float64)
        )
    else:
        out = out.with_columns(final_score=pl.col("base_score").cast(pl.Float64))

    # Regra de negócio: selecionar telefones distintos.
    # Um mesmo telefone pode aparecer em múltiplos sistemas e datas de atualização.
    # Mantemos, por telefone, o melhor registro (maior final_score) e, em empate, o mais recente.
    out = (
        out.sort(["final_score", "updated_at_dt"], descending=True)
        .group_by(phone_col)
        .first()
    )

    return (
        out.sort("final_score", descending=True)
        .with_columns(rank=pl.int_range(1, pl.len() + 1))
        .select([phone_col, system_col, updated_at_col] + ([ddd_col] if ddd_col else []) + ["final_score", "rank"])
    )

