# presas-mexico

Pipeline de datos para monitorear las 210 presas principales de México. Descarga histórico diario desde CONAGUA SIH (1944-presente), enriquece con NAMO/NAME oficiales, transforma con dbt-duckdb, y exporta parquets listos para consumo desde el browser vía DuckDB-WASM.

**Datos validados contra CONAGUA/CIDH al ±0.1%.**

## Stack

- **Ingesta**: Python 3.13, httpx (async), DuckDB
- **Transformación**: dbt-duckdb 1.10, dbt_utils
- **Orquestación**: scripts CLI con uv, watermark-based incremental
- **Export**: parquet con compresión zstd, particionado por estado
- **Editor**: PyCharm + Neovim, uv para entornos

## Arquitectura

CONAGUA SIH (CSV por presa)
↓ fetch\_incremental.py (210 paralelos, tail-trim 60 días)
data/raw/latest/
↓ raw\_to\_duckdb.py (watermark por dam\_key, UPSERT)
warehouse.duckdb · raw.presas\_diario  (\~4M filas, 1944-2026)
↓ dbt build (staging → dim → fct → snapshot)
warehouse.duckdb · main\_marts.\*
↓ parquet\_writer.py
data/exports/
├── current/        (sobrescrito)
│   ├── dim\_presa.parquet
│   ├── snapshot\_actual.parquet
│   └── fct\_by\_state/state=<X>.parquet  (26 estados)
├── archive/YYYY-MM/  (snapshot mensual)
└── manifest.json


## Fuentes de datos


| Fuente                                     | Qué da                                                       | Frecuencia                        |
| ------------------------------------------ | ------------------------------------------------------------- | --------------------------------- |
| [CONAGUA SIH](https://sih.conagua.gob.mx/) | Almacenamiento diario por presa, histórico desde los 40s-90s | Diaria (~1 día de retraso)       |
| CONAGUA SIH (catálogo xls)                | Coordenadas, municipio, cuenca, región hidrológica          | Estático (último update 2020)   |
| CONAGUA SINAV30                            | NAMO/NAME oficiales, cotas, año de construcción, uso, río  | Estático (dataset al 2025-04-23) |

El SIH publica un CSV por presa con header de 7 líneas de metadata + columnas inconsistentes entre presas. El script `raw_to_duckdb.py` lo normaliza por nombre de columna (no posición) y soporta UPSERT.

## Setup

```bash
# Python 3.13 + uv
uv sync

# Configurar dbt profile path
export DBT_PROFILES_DIR=./presas_mx_dbt
```

## Uso

**Inicialización** (una sola vez):

```bash
# 1. Generar seed del catálogo desde el xls oficial
uv run python scripts/build_seed.py

# 2. Enriquecer con NAMO/NAME oficiales de SINAV30
uv run python scripts/enrich_catalog_capacity.py

# 3. Cargar seed a DuckDB
cd presas_mx_dbt && dbt seed && cd ..

# 4. Backfill histórico completo (~4M filas, ~10 min)
uv run python -m ingest.fetch_backfill
uv run python -m ingest.raw_to_duckdb --source data/raw/backfill_2026-06-27

# 5. Construir marts
cd presas_mx_dbt && dbt build && cd ..

# 6. Exportar parquets
uv run python -m export.parquet_writer --force-archive
```

**Operación diaria**:

```bash
uv run python -m ingest.fetch_incremental
uv run python -m ingest.raw_to_duckdb
cd presas_mx_dbt && dbt build && cd ..
uv run python -m export.parquet_writer
```

## Modelos dbt

- `stg_presas_diario` — limpieza, filtro de fechas centinela (<1940), volúmenes negativos.
- `stg_catalogo` — wrapper sobre el seed.
- `dim_presa` — catálogo + métricas históricas derivadas (P99 como NAMO estimado de respaldo).
- `fct_presa_diario` — incremental, grano (presa, fecha), agrega `pct_llenado` (vs NAMO) y `pct_llenado_name` (vs NAME), lag y delta diaria.
- `snapshot_actual` — view de la última fila por presa.

Run `dbt docs serve` desde `presas_mx_dbt/` para lineage gráfico.

## Limitaciones conocidas

- **3 presas con schema variante** (CUQJL, ESTJL, VCVJL en Jalisco): usan nombres de columnas con acentos (`Precipitación`, `Almacenamiento(hm³)`) distintos al resto. No están cargadas. Total: 207/210 funcionales. *Issue abierto.*
- **NAMO/NAME estáticos**: el dataset de SINAV30 es al 2025-04-23. Las capacidades físicas casi nunca cambian (excepto recalibración topobatimétrica cada 10-20 años por presa).
- **Algunas presas con datos rezagados**: ~5 presas reportan al SIH con retraso >30 días. Visible en `last_record_date` de `dim_presa`.

## Estructura del repo

`
├── ingest/                    # scripts de descarga y carga
│   ├── fetch\_backfill.py      # corrida inicial, histórico completo
│   ├── fetch\_incremental.py   # diario, tail-trim 60 días
│   └── raw\_to\_duckdb.py       # CSV → DuckDB con watermark + UPSERT
├── export/
│   └── parquet\_writer.py      # marts → parquet zstd particionado
├── scripts/
│   ├── build\_seed.py          # genera seed desde xls oficial
│   └── enrich\_catalog\_capacity.py  # agrega NAMO/NAME a seed
├── presas\_mx\_dbt/             # proyecto dbt
│   ├── seeds/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── dbt\_project.yml
├── data/                      # gitignored
│   ├── catalog/
│   ├── raw/
│   └── exports/
├── pyproject.toml
└── README.md

`


## Frontend

El dashboard que consume estos parquets está en [`presas-mexico-webpage`](https://presas-mexico-webpage.vercel.app/).

## Licencia

MIT. Datos: dominio público (CONAGUA).
