"""
Descarga el reporte de SINAV30 (datos de capacidades oficiales CONAGUA al 2025-04-23)
y enriquece el seed con NAMO/NAME oficiales por clavesih.

Uso: uv run python scripts/enrich_seed_with_namo.py
"""
from __future__ import annotations

import json
from pathlib import Path

import httpx
import pandas as pd

SINAV_URL = "https://sinav30.conagua.gob.mx:8080/PresasPG/presas/reporte/2025-04-23"
SEED_PATH = Path("presas_mx_dbt/seeds/presas_catalogo.csv")
RAW_PATH = Path("data/catalog/sinav_reporte.json")

# headers de navegador para evitar el WAF Imperva
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:151.0) "
        "Gecko/20100101 Firefox/151.0"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
    "Referer": "https://sinav30.conagua.gob.mx:8080/Presas/",
}


def download() -> list[dict]:
    print(f"Descargando {SINAV_URL}...")
    with httpx.Client(timeout=60.0, headers=HEADERS, follow_redirects=True) as client:
        r = client.get(SINAV_URL)
        r.raise_for_status()
        data = r.json()
    print(f"✓ {len(data)} registros recibidos")

    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    RAW_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    return data


def enrich() -> None:
    raw = download()
    sinav = pd.DataFrame(raw)

    # nos quedamos solo con las columnas que valen la pena
    sinav = sinav[[
        "clavesih",
        "namoalmac", "nameelev", "namealmac", "namoelev",
        "alturacortina", "elevcorona", "inicioop",
        "corriente", "uso", "tipovertedor",
        "nombrecomun",
    ]].rename(columns={
        "clavesih": "dam_key",
        "namoalmac": "namo_oficial_mm3",
        "namealmac": "name_oficial_mm3",
        "namoelev":  "namo_elev_msnm",
        "nameelev":  "name_elev_msnm",
        "alturacortina": "altura_cortina_m",
        "elevcorona": "elev_corona_msnm",
        "inicioop": "inicio_operacion",
        "corriente": "corriente",
        "uso": "uso",
        "tipovertedor": "tipo_vertedor",
        "nombrecomun": "nombre_comun",
    })

    # tipos numéricos donde aplique
    for col in ("namo_oficial_mm3", "name_oficial_mm3", "namo_elev_msnm",
                "name_elev_msnm", "altura_cortina_m", "elev_corona_msnm"):
        sinav[col] = pd.to_numeric(sinav[col], errors="coerce")

    sinav["inicio_operacion"] = pd.to_numeric(
        sinav["inicio_operacion"], errors="coerce"
    ).astype("Int64")

    # leer seed actual
    seed = pd.read_csv(SEED_PATH)
    print(f"Seed actual: {len(seed)} presas, columnas: {seed.columns.tolist()}")

    # merge por dam_key
    merged = seed.merge(sinav, on="dam_key", how="left")

    # reportar matches
    matched = merged["namo_oficial_mm3"].notna().sum()
    print(f"\nMatch por clavesih: {matched}/{len(seed)}")

    unmatched = merged[merged["namo_oficial_mm3"].isna()]
    if len(unmatched) > 0:
        print(f"\nSin match ({len(unmatched)}):")
        for _, r in unmatched.iterrows():
            print(f"  {r['dam_key']:12} {r['dam_name_clean']}")

    # validación: comparar P99 vs NAMO oficial para presas grandes
    print("\n--- Comparación P99 estimado vs NAMO oficial (top 10) ---")
    if "namo_estimado_mm3" in merged.columns:
        comp = merged.dropna(subset=["namo_oficial_mm3", "namo_estimado_mm3"]).copy()
        comp["diff_pct"] = (
            (comp["namo_estimado_mm3"] - comp["namo_oficial_mm3"])
            / comp["namo_oficial_mm3"] * 100
        )
        comp = comp.nlargest(10, "namo_oficial_mm3")
        for _, r in comp.iterrows():
            print(
                f"  {r['dam_key']:10} {r['dam_name_clean'][:25]:25} "
                f"oficial={r['namo_oficial_mm3']:8.1f}  "
                f"P99={r['namo_estimado_mm3']:8.1f}  "
                f"diff={r['diff_pct']:+5.1f}%"
            )

    merged.to_csv(SEED_PATH, index=False)
    print(f"\n✓ Seed enriquecido: {SEED_PATH}")
    print(f"  Columnas finales: {merged.columns.tolist()}")


if __name__ == "__main__":
    enrich()