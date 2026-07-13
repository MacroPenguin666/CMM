"""
Shared parquet landing helper for the data-ingestion-pipeline.

This is the spine every raw fetcher writes through. Design goals:

  * **Raw and in full** — store exactly what the upstream source returns, plus a
    handful of provenance columns. No transformation, no schema coercion.
  * **Append-only** — every run writes a NEW part file; we never overwrite or
    upsert. A value revised by the source on a later run is appended as a fresh
    row with a later ``_ingested_at``. The full revision (vintage) history is the
    union of all part files. "Latest value" is a *read-time* concept
    (see :func:`latest_view`), never a write-time one.
  * **Self-contained** — resolves its own paths under ``02_inputs/`` and does NOT
    import ``backend.storage`` or touch ``data/cmm.db``. The two pipelines run in
    parallel.

Layout written:

    02_inputs/<source>/<dataset>/ingest_date=YYYY-MM-DD/<run_id>.parquet
    02_inputs/<source>/_manifest.json

Provenance columns stamped on every row:

    _source       source id (e.g. "nbs")
    _dataset      dataset id within the source (e.g. "series", "catalog")
    _ingested_at  UTC ISO-8601 timestamp of the fetch
    _run_id       id shared by all datasets written in one run (sortable)
    _endpoint     upstream URL / interface the rows came from (optional)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

log = logging.getLogger("raw_store")

# 02_inputs is the sibling of 01_raw_fetchers (this file lives in 01_raw_fetchers/)
RAW_ROOT = Path(__file__).resolve().parents[1] / "02_inputs"

PROVENANCE_COLS = ["_source", "_dataset", "_ingested_at", "_run_id", "_endpoint"]


def new_run_id() -> str:
    """A sortable run id shared across all datasets written in one fetch run."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append(
    source: str,
    dataset: str,
    df: pd.DataFrame,
    *,
    run_id: str,
    endpoint: str | None = None,
) -> Path | None:
    """
    Append ``df`` to ``02_inputs/<source>/<dataset>/`` as a new part file.

    Returns the path written, or ``None`` if ``df`` is empty (nothing written).
    Provenance columns are added; the caller's own columns are preserved as-is.
    """
    if df is None or len(df) == 0:
        log.info("  skip %s/%s — empty frame", source, dataset)
        return None

    out = df.copy()
    out["_source"] = source
    out["_dataset"] = dataset
    out["_ingested_at"] = _utc_now_iso()
    out["_run_id"] = run_id
    out["_endpoint"] = endpoint

    ingest_date = run_id[:8]  # YYYYMMDD prefix of the run id
    part_dir = RAW_ROOT / source / dataset / f"ingest_date={ingest_date[:4]}-{ingest_date[4:6]}-{ingest_date[6:8]}"
    part_dir.mkdir(parents=True, exist_ok=True)
    part_path = part_dir / f"{run_id}.parquet"

    # If two datasets in the same run collide on path (shouldn't), suffix.
    if part_path.exists():
        i = 1
        while (part_dir / f"{run_id}.{i}.parquet").exists():
            i += 1
        part_path = part_dir / f"{run_id}.{i}.parquet"

    try:
        out.to_parquet(part_path, engine="pyarrow", index=False)
    except Exception as e:
        # Mixed-type object columns (common in scraped tables / spreadsheets) break
        # Arrow's type inference. Stringify object columns and retry — raw landing
        # keeps the bytes faithfully; downstream layers re-type as needed.
        log.warning("  parquet retry for %s/%s after %s — stringifying object cols",
                    source, dataset, type(e).__name__)
        for c in out.columns:
            if out[c].dtype == object:
                out[c] = out[c].map(lambda v: None if v is None else str(v))
        out.to_parquet(part_path, engine="pyarrow", index=False)
    log.info("  wrote %d rows -> %s", len(out), part_path.relative_to(RAW_ROOT.parent))
    return part_path


def read_dataset(source: str, dataset: str) -> pd.DataFrame:
    """Read the full partitioned dataset (all vintages) into one DataFrame."""
    root = RAW_ROOT / source / dataset
    if not root.exists():
        return pd.DataFrame()
    parts = sorted(root.rglob("*.parquet"))
    if not parts:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(p, engine="pyarrow") for p in parts), ignore_index=True)


def latest_view(source: str, dataset: str, key_cols: list[str]) -> pd.DataFrame:
    """
    Collapse all vintages to the most recently ingested row per ``key_cols``.
    For spot-checking / downstream use — the raw parts on disk are untouched.
    """
    df = read_dataset(source, dataset)
    if df.empty:
        return df
    df = df.sort_values("_ingested_at")
    return df.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def write_manifest(
    source: str,
    *,
    status: str,
    datasets: dict[str, int] | None = None,
    run_id: str | None = None,
    error: str | None = None,
) -> None:
    """
    Write/refresh ``02_inputs/<source>/_manifest.json`` — the parquet-layer
    analogue of the cmm.db ``fetch_log`` table. ``datasets`` maps dataset name to
    rows written this run.
    """
    src_dir = RAW_ROOT / source
    src_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "source": source,
        "last_run_id": run_id,
        "last_run_at": _utc_now_iso(),
        "status": status,
        "datasets": datasets or {},
        "rows_total": sum((datasets or {}).values()),
        "error": error,
    }
    (src_dir / "_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("manifest: %s status=%s rows=%d", source, status, manifest["rows_total"])
