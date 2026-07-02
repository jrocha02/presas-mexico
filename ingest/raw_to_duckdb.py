"""
Carga los CSVs descargados a raw.presas_diario en warehouse.duckdb.

Las 210 CSVs tienen estas peculiaridades:
- 4 líneas de header con metadata (Comisión, Estación, Clave, Estado/Municipio)
- Orden de columnas inconsistente entre presas (VolumenAlm puede estar en col 4 o col 7)
- Algunas tienen columnas extra o nombres ligeramente distintos

Estrategia: leer cada CSV individualmente con DuckDB (que es robusto a esto),
seleccionar columnas por nombre, y hacer UNION ALL.

Uso:
    uv run python -m ingest.raw_to_duckdb
    uv run python -m ingest.raw_to_duckdb --date 2026-06-27
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import duckdb

WAREHOUSE = Path("warehouse.duckdb")
RAW_DIR = Path("data/raw")

EXPECTED_COLS = [
    "Estacion",
    "Fecha",
    "Precipitacion(mm)",
    "Evaporacion(mm)",
    "ObraToma(m3/s)",
    "Vertedor(m3/s)",
    "Derrame(m3/s)",
    "VolumenAlm(Mm3)",
]
LATEST_DIR = Path("data/raw/latest")


def load(source_dir: Path | None = None) -> None:
    if source_dir is None:
        source_dir = LATEST_DIR  # data/raw/latest

    if not source_dir.exists():
        raise FileNotFoundError(f"No existe {source_dir}. Corre fetch primero.")

    csvs = sorted(source_dir.glob("*.csv"))
    print(f"Encontrados {len(csvs)} CSVs en {source_dir}")

    con = duckdb.connect(str(WAREHOUSE))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.presas_diario (
            dam_key            VARCHAR NOT NULL,
            fecha              DATE NOT NULL,
            precipitacion_mm   DOUBLE,
            evaporacion_mm     DOUBLE,
            obra_toma_m3s      DOUBLE,
            vertedor_m3s       DOUBLE,
            derrame_m3s        DOUBLE,
            volumen_alm_mm3    DOUBLE,
            ingested_at        TIMESTAMP DEFAULT now(),
            PRIMARY KEY (dam_key, fecha)
        )
    """)

    # watermark: fecha máxima por presa
    watermarks = dict(con.execute("""
        SELECT dam_key, MAX(fecha) FROM raw.presas_diario GROUP BY dam_key
    """).fetchall())
    print(f"Watermarks existentes: {len(watermarks)} presas")

    con.execute("DROP TABLE IF EXISTS raw._presas_staging")
    con.execute("""
        CREATE TABLE raw._presas_staging (
            dam_key VARCHAR, fecha DATE,
            precipitacion_mm DOUBLE, evaporacion_mm DOUBLE,
            obra_toma_m3s DOUBLE, vertedor_m3s DOUBLE,
            derrame_m3s DOUBLE, volumen_alm_mm3 DOUBLE
        )
    """)

    ok, fail = 0, []
    for i, csv in enumerate(csvs, 1):
        dam_key = csv.stem
        watermark = watermarks.get(dam_key)
        where_watermark = (
            f"AND CAST(\"Fecha\" AS DATE) > DATE '{watermark}'"
            if watermark else ""
        )
        try:
            con.execute(f"""
                INSERT INTO raw._presas_staging
                SELECT
                    '{dam_key}' AS dam_key,
                    CAST("Fecha" AS DATE) AS fecha,
                    TRY_CAST("Precipitacion(mm)"  AS DOUBLE),
                    TRY_CAST("Evaporacion(mm)"    AS DOUBLE),
                    TRY_CAST("ObraToma(m3/s)"     AS DOUBLE),
                    TRY_CAST("Vertedor(m3/s)"     AS DOUBLE),
                    TRY_CAST("Derrame(m3/s)"      AS DOUBLE),
                    TRY_CAST("VolumenAlm(Mm3)"    AS DOUBLE)
                FROM read_csv(
                    '{csv.as_posix()}',
                    skip = 7, header = true, ignore_errors = true,
                    nullstr = ['', 'NULL', 'null']
                )
                WHERE "Fecha" IS NOT NULL
                  {where_watermark}
            """)
            ok += 1
        except Exception as e:
            fail.append((dam_key, str(e)[:200]))

    new_rows = con.execute("SELECT COUNT(*) FROM raw._presas_staging").fetchone()[0]
    print(f"\nFilas nuevas en staging: {new_rows:,}")

    con.execute("""
        INSERT INTO raw.presas_diario
            (dam_key, fecha, precipitacion_mm, evaporacion_mm,
             obra_toma_m3s, vertedor_m3s, derrame_m3s, volumen_alm_mm3)
        SELECT dam_key, fecha, precipitacion_mm, evaporacion_mm,
               obra_toma_m3s, vertedor_m3s, derrame_m3s, volumen_alm_mm3
        FROM raw._presas_staging
        ON CONFLICT (dam_key, fecha) DO UPDATE SET
            precipitacion_mm = EXCLUDED.precipitacion_mm,
            evaporacion_mm   = EXCLUDED.evaporacion_mm,
            obra_toma_m3s    = EXCLUDED.obra_toma_m3s,
            vertedor_m3s     = EXCLUDED.vertedor_m3s,
            derrame_m3s      = EXCLUDED.derrame_m3s,
            volumen_alm_mm3  = EXCLUDED.volumen_alm_mm3,
            ingested_at      = now()
    """)
    con.execute("DROP TABLE raw._presas_staging")

    total = con.execute("SELECT COUNT(*) FROM raw.presas_diario").fetchone()[0]
    max_date = con.execute("SELECT MAX(fecha) FROM raw.presas_diario").fetchone()[0]
    print(f"\n--- raw.presas_diario ---")
    print(f"Filas totales:  {total:,}")
    print(f"Fecha más reciente: {max_date}")
    if fail:
        print(f"\n✗ Fallidos ({len(fail)}):")
        for k, m in fail:
            print(f"  {k}: {m}")
    con.close()
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source",
        type=Path,
        default=LATEST_DIR,
        help="Directorio con los CSVs a cargar. Default: data/raw/latest/",
    )
    args = parser.parse_args()
    load(args.source)