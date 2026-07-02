"""
Descarga el catálogo oficial de CONAGUA y lo convierte en seed de dbt.
Uso: uv run python scripts/build_seed.py
"""

from pathlib import Path
import httpx
import pandas as pd

CATALOG_URL = "https://sih.conagua.gob.mx/basedatos/Presas/0_Catalogo_de_presas.xls"
SEED_PATH = Path("presas_mx_dbt/seeds/presas_catalogo.csv")
RAW_PATH = Path("data/catalog/catalogo_crudo.xls")


def download() -> Path:
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        r = client.get(CATALOG_URL)
        r.raise_for_status()
        RAW_PATH.write_bytes(r.content)
    print(f"✓ Descargado: {RAW_PATH} ({len(r.content):,} bytes)")
    return RAW_PATH


def inspect(path: Path) -> pd.DataFrame:
    # los .xls "viejos" de gob.mx a veces son HTML disfrazado o xlsx con extensión mentirosa.
    # probamos en orden: xlrd → openpyxl → read_html como fallback.
    for engine in ("xlrd", "openpyxl"):
        try:
            df = pd.read_excel(path, engine=engine)
            print(f"✓ Parseado con engine={engine}")
            break
        except Exception as e:
            print(f"  engine={engine} falló: {e}")
    else:
        # último intento: HTML
        df = pd.read_html(path)[0]
        print("✓ Parseado como HTML")

    print(f"\nColumnas detectadas ({len(df.columns)}):")
    for c in df.columns:
        print(f"  - {c!r}")
    print(f"\nPrimeras 3 filas:")
    print(df.head(3).to_string())
    print(f"\nTotal filas: {len(df)}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # paso 0: normaliza nombres de columnas (strip + colapsa whitespace incluyendo \n)
    df.columns = [" ".join(c.split()).strip() for c in df.columns]

    rename_map = {
        "Número": "numero",
        "Clave": "dam_key",
        "Nombre de la presa": "dam_name",
        "Latitud": "latitude",
        "Longitud": "longitude",
        "Altitud": "altitude_m",
        "Estado": "state",
        "Municipio": "municipality",
        "Identificador de la cuenca de disponibilidad": "basin_id",
        "Cuenca de disponibilidad": "basin_name",
        "Número de la región hidrológica": "hydro_region_id",
        "Región hidrológica": "hydro_region_name",
    }
    df = df.rename(columns=rename_map)

    # assertion para fallar rápido si algo no matcheó
    expected = set(rename_map.values())
    missing = expected - set(df.columns)
    assert not missing, f"Columnas no renombradas: {missing}. Columnas actuales: {df.columns.tolist()}"

    # tipos numéricos
    for col in ("latitude", "longitude", "altitude_m"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("numero", "basin_id", "hydro_region_id"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # strings
    for col in ("dam_key", "dam_name", "state", "municipality", "basin_name", "hydro_region_name"):
        df[col] = df[col].astype(str).str.strip()

    df["dam_name_clean"] = df["dam_name"].str.replace(r",\s*[A-Z][a-z.]+\.?\s*$", "", regex=True)
    df["state_code"] = df["dam_key"].str[-2:]

    cols = [
        "numero", "dam_key", "state_code",
        "dam_name", "dam_name_clean",
        "state", "municipality",
        "latitude", "longitude", "altitude_m",
        "basin_id", "basin_name",
        "hydro_region_id", "hydro_region_name",
    ]
    return df[cols]



def write_seed(df: pd.DataFrame) -> None:
    SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(SEED_PATH, index=False)
    print(f"\n✓ Seed escrito: {SEED_PATH} ({len(df)} filas)")


if __name__ == "__main__":
    path = download()
    df = inspect(path)
    df = clean(df)
    write_seed(df)

