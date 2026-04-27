from __future__ import annotations

from pathlib import Path

import polars as pl


def _pick_parquet_path(data_raw: Path, table_name: str) -> Path:
    direct = data_raw / table_name
    if direct.exists() and direct.is_file():
        return direct

    candidates = sorted(data_raw.rglob(f"*{table_name}*.parquet"))
    if not candidates:
        candidates = sorted(data_raw.rglob(f"*{table_name}*"))
        candidates = [p for p in candidates if p.is_file() and p.name != "schema.yml"]
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado para {table_name} em {data_raw}. "
            f"Baixe os dados do GCS para data/raw antes de executar."
        )
    return candidates[0]


def load_base_disparo(data_raw: Path) -> pl.LazyFrame:
    path = _pick_parquet_path(data_raw, "base_disparo_mascarado")
    return pl.scan_parquet(path)


def load_dim_telefone(data_raw: Path) -> pl.LazyFrame:
    path = _pick_parquet_path(data_raw, "dim_telefone_mascarado")
    return pl.scan_parquet(path)

