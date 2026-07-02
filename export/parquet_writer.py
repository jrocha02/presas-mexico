"""
Exporta los marts de DuckDB a parquet local.
Estructura:
  data/exports/current/                 # se sobrescribe en cada corrida
    ├── dim_presa.parquet
    ├── snapshot_actual.parquet
    └── fct_by_state/state=<X>.parquet
  data/exports/archive/YYYY-MM/         # se escribe 1x al mes (día 1)
    └── snapshot.parquet
  data/exports/manifest.json            # metadata para el frontend

Uso:
    uv run python -m export.parquet_writer
    uv run python -m export.parquet_writer --force-archive
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

WAREHOUSE = Path("warehouse.duckdb")
EXPORTS_DIR = Path("data/exports")
CURRENT_DIR = EXPORTS_DIR / "current"
ARCHIVE_DIR = EXPORTS_DIR / "archive"


def write_parquet(con: duckdb.DuckDBPyConnection, query: str, out_path: Path) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY ({query}) TO '{out_path.as_posix()}' "
        "(FORMAT 'parquet', COMPRESSION 'zstd')"
    )
    return out_path.stat().st_size


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def export_current(con: duckdb.DuckDBPyConnection) -> dict:
    # limpia para evitar parquet huérfanos (estados renombrados, presas removidas, etc.)
    if CURRENT_DIR.exists():
        shutil.rmtree(CURRENT_DIR)
    CURRENT_DIR.mkdir(parents=True)

    files = {}

    size = write_parquet(
        con,
        "SELECT * FROM main_marts.dim_presa",
        CURRENT_DIR / "dim_presa.parquet",
    )
    files["dim_presa.parquet"] = {
        "size_bytes": size,
        "hash": file_hash(CURRENT_DIR / "dim_presa.parquet"),
    }
    print(f"  ✓ dim_presa.parquet ({size:,} bytes)")

    size = write_parquet(
        con,
        "SELECT * FROM main_marts.snapshot_actual",
        CURRENT_DIR / "snapshot_actual.parquet",
    )
    files["snapshot_actual.parquet"] = {
        "size_bytes": size,
        "hash": file_hash(CURRENT_DIR / "snapshot_actual.parquet"),
    }
    print(f"  ✓ snapshot_actual.parquet ({size:,} bytes)")

    # fct_presa_diario particionado por estado
    states = [r[0] for r in con.execute("""
        SELECT DISTINCT state FROM main_marts.dim_presa
        WHERE state IS NOT NULL ORDER BY state
    """).fetchall()]

    fct_dir = CURRENT_DIR / "fct_by_state"
    fct_dir.mkdir(exist_ok=True)
    fct_files = {}
    for state in states:
        safe_name = state.replace(" ", "_").replace("/", "_")
        out = fct_dir / f"state={safe_name}.parquet"
        size = write_parquet(
            con,
            f"""
            SELECT * FROM main_marts.fct_presa_diario
            WHERE state = '{state.replace("'", "''")}'
            ORDER BY dam_key, fecha
            """,
            out,
        )
        fct_files[f"fct_by_state/state={safe_name}.parquet"] = {
            "size_bytes": size,
            "hash": file_hash(out),
            "state": state,
        }
    print(f"  ✓ fct_by_state/ ({len(fct_files)} estados)")
    files.update(fct_files)

    return files


def export_archive(con: duckdb.DuckDBPyConnection, force: bool = False) -> dict | None:
    today = date.today()
    if today.day != 1 and not force:
        print("  (skip: no es día 1, usa --force-archive para forzar)")
        return None

    month_key = today.strftime("%Y-%m")
    out_dir = ARCHIVE_DIR / month_key
    out_dir.mkdir(parents=True, exist_ok=True)

    out = out_dir / "snapshot.parquet"
    size = write_parquet(con, "SELECT * FROM main_marts.snapshot_actual", out)
    print(f"  ✓ archive/{month_key}/snapshot.parquet ({size:,} bytes)")
    return {
        "month": month_key,
        "size_bytes": size,
        "hash": file_hash(out),
    }


def write_manifest(
    con: duckdb.DuckDBPyConnection,
    current_files: dict,
    archive_entry: dict | None,
) -> None:
    stats = con.execute("""
        SELECT
            (SELECT COUNT(*) FROM main_marts.snapshot_actual)    AS total_presas,
            (SELECT MAX(fecha) FROM main_marts.fct_presa_diario) AS data_max_fecha,
            (SELECT MIN(fecha) FROM main_marts.fct_presa_diario) AS data_min_fecha
    """).fetchone()

    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "data_min_fecha": stats[2].isoformat() if stats[2] else None,
        "data_max_fecha": stats[1].isoformat() if stats[1] else None,
        "total_presas": stats[0],
        "current": current_files,
        "latest_archive": archive_entry,
    }

    manifest_path = EXPORTS_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(f"\n✓ manifest.json escrito")
    print(f"  Presas: {manifest['total_presas']}")
    print(f"  Rango:  {manifest['data_min_fecha']} → {manifest['data_max_fecha']}")


def main(force_archive: bool = False) -> None:
    if not WAREHOUSE.exists():
        raise FileNotFoundError(f"No existe {WAREHOUSE}. Corre el pipeline primero.")

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)

    print("→ Exportando current/")
    current_files = export_current(con)

    print("\n→ Verificando archive/")
    archive_entry = export_archive(con, force=force_archive)

    print("\n→ Escribiendo manifest.json")
    write_manifest(con, current_files, archive_entry)

    con.close()

    total = sum(f.stat().st_size for f in EXPORTS_DIR.rglob("*") if f.is_file())
    print(f"\nTamaño total data/exports/: {total / 1_048_576:.2f} MB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force-archive", action="store_true",
        help="Genera archive aunque no sea día 1",
    )
    args = parser.parse_args()
    main(force_archive=args.force_archive)