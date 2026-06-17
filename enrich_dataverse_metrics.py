"""
Local enrichment script — run on your laptop (no firewall restrictions).

Reads agent1_results.csv and backfills for every Dataverse record:
  - downloads_count  (makeDataCount/downloadsTotal)
  - total_views      (makeDataCount/viewsTotal)
  - dataset_type     (spatial | tabular | unstructured) from the files API

Metrics and type detection are independent — a rate-limited metrics call does NOT
block type detection for the same record.

On 403 (rate limit), the script backs off for 10 seconds and retries once.

Writes results back to the same CSV (original backed up as agent1_results.csv.bak).

Usage:
    python enrich_dataverse_metrics.py
    python enrich_dataverse_metrics.py --csv path/to/other.csv --dry-run
"""

import argparse
import shutil
import time
import requests
import pandas as pd
from pathlib import Path

DATAVERSE_BASE = "https://dataverse.harvard.edu"

_SPATIAL_TYPES = {
    'image/tiff', 'image/geotiff', 'application/x-netcdf',
    'application/geo+json', 'application/vnd.geo+json',
    'application/x-hdf', 'application/x-hdf5',
}
_SPATIAL_EXTS  = {'.tif', '.tiff', '.geotiff', '.shp', '.geojson', '.kml',
                  '.gpkg', '.nc', '.img', '.grd', '.dem', '.asc', '.prj', '.dbf'}
_TABULAR_TYPES = {
    'text/csv', 'text/tab-separated-values', 'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/x-stata', 'application/x-stata-dta',
    'application/x-spss-sav', 'application/x-r-data',
}
_TABULAR_EXTS  = {'.csv', '.tsv', '.tab', '.xlsx', '.xls', '.dta', '.sav', '.rdata', '.rds'}


def fetch_metric(doi: str, metric: str) -> int | None:
    """Fetch downloadsTotal or viewsTotal. Returns None on any failure."""
    url = f"{DATAVERSE_BASE}/api/datasets/:persistentId/makeDataCount/{metric}?persistentId={doi}"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 403:
            print(f"\n    [rate-limited on {metric}, backing off 10s]", end="", flush=True)
            time.sleep(10)
            r = requests.get(url, timeout=30)  # one retry after backoff
        r.raise_for_status()
        return int(r.json().get("data", {}).get(metric) or 0)
    except Exception:
        return None


def detect_type(doi: str) -> str | None:
    """Classify dataset as spatial, tabular, or unstructured. Returns None on failure."""
    url = f"{DATAVERSE_BASE}/api/datasets/:persistentId/versions/:latest-published/files?persistentId={doi}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        files = r.json().get("data", [])
    except Exception:
        return None

    has_tabular = False
    for f in files:
        df_ = f.get("dataFile", {})
        ct  = (df_.get("contentType") or "").lower()
        fn  = (df_.get("filename") or "").lower()
        ext = ("." + fn.rsplit(".", 1)[-1]) if "." in fn else ""

        if ct in _SPATIAL_TYPES or ext in _SPATIAL_EXTS:
            return "spatial"
        if df_.get("tabularData") or ct in _TABULAR_TYPES or ext in _TABULAR_EXTS:
            has_tabular = True

    return "tabular" if has_tabular else "unstructured"


def enrich_types_only(csv_path: Path, dry_run: bool = False):
    """Only classify dataset_type — leaves downloads_count and total_views untouched."""
    print(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)

    if "dataset_type" not in df.columns:
        df["dataset_type"] = "unknown"

    mask = df["repository_source"].str.strip().str.lower() == "dataverse"
    needs_update = mask & (df["dataset_type"].fillna("unknown") == "unknown")
    rows = df[needs_update]
    total = len(rows)
    print(f"Found {total} Dataverse records needing dataset_type classification.\n")

    updated = 0
    failed  = 0
    for i, (idx, row) in enumerate(rows.iterrows(), start=1):
        doi = str(row.get("doi_pid", "")).strip()
        if not doi:
            continue

        if not doi.lower().startswith("doi:"):
            doi_param = f"doi:{doi}"
        else:
            doi_param = doi

        print(f"  [{i}/{total}] {doi_param[:65]}", end="  ", flush=True)

        dtype = detect_type(doi_param)
        if dtype is not None:
            print(f"type={dtype}")
            if not dry_run:
                df.at[idx, "dataset_type"] = dtype
            updated += 1
        else:
            print("type=ERR")
            failed += 1

        time.sleep(0.5)

    if not dry_run:
        backup = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup)
        print(f"\nBackup saved → {backup}")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"Enriched CSV saved → {csv_path}")
    else:
        print("\nDry-run mode — no files written.")

    print(f"\nDone. {updated} classified, {failed} failed out of {total} records.")


def enrich(csv_path: Path, dry_run: bool = False):
    print(f"Reading {csv_path} ...")
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)

    for col, default in [("downloads_count", 0), ("total_views", 0), ("dataset_type", "unknown")]:
        if col not in df.columns:
            df[col] = default

    mask = df["repository_source"].str.strip().str.lower() == "dataverse"
    rows = df[mask]
    total = len(rows)
    print(f"Found {total} Dataverse records to enrich.\n")

    updated = 0
    for i, (idx, row) in enumerate(rows.iterrows(), start=1):
        doi = str(row.get("doi_pid", "")).strip()
        if not doi:
            continue

        if not doi.lower().startswith("doi:"):
            doi_param = f"doi:{doi}"
        else:
            doi_param = doi

        print(f"  [{i}/{total}] {doi_param[:65]}", end="  ", flush=True)

        downloads = fetch_metric(doi_param, "downloadsTotal")
        views     = fetch_metric(doi_param, "viewsTotal")
        dtype     = detect_type(doi_param)

        # Report what we got (None means that call failed)
        d_str = f"down={downloads:,}" if downloads is not None else "down=ERR"
        v_str = f"views={views:,}"    if views     is not None else "views=ERR"
        t_str = f"type={dtype}"       if dtype     is not None else "type=ERR"
        print(f"{d_str}  {v_str}  {t_str}")

        if not dry_run:
            if downloads is not None:
                df.at[idx, "downloads_count"] = downloads
            if views is not None:
                df.at[idx, "total_views"] = views
            if dtype is not None:
                df.at[idx, "dataset_type"] = dtype

        # Count as updated if at least one field succeeded
        if any(x is not None for x in [downloads, views, dtype]):
            updated += 1

        time.sleep(0.8)  # respectful pacing

    if not dry_run:
        backup = csv_path.with_suffix(".csv.bak")
        shutil.copy2(csv_path, backup)
        print(f"\nBackup saved → {backup}")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"Enriched CSV saved → {csv_path}")
    else:
        print("\nDry-run mode — no files written.")

    print(f"\nDone. {updated}/{total} records had at least one field enriched.")


if __name__ == "__main__":
    default_csv = Path(__file__).parent / "cgiar_mas_agent1" / "output" / "agent1_results.csv"

    parser = argparse.ArgumentParser(description="Enrich Dataverse metrics and dataset types locally.")
    parser.add_argument("--csv",       type=Path, default=default_csv, help="Path to agent1_results.csv")
    parser.add_argument("--dry-run",   action="store_true", help="Fetch and print only — do not write")
    parser.add_argument("--type-only", action="store_true", help="Only classify dataset_type, skip metrics")
    args = parser.parse_args()

    if args.type_only:
        enrich_types_only(args.csv, dry_run=args.dry_run)
    else:
        enrich(args.csv, dry_run=args.dry_run)
