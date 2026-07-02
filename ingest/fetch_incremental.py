"""
Descarga incremental: baja las 210 CSVs del SIH, las guarda en data/raw/latest/
(se sobrescribe cada corrida), y deja que raw_to_duckdb filtre por watermark.

Uso:
    uv run python -m ingest.fetch_incremental
"""
from __future__ import annotations

import asyncio
import shutil
from datetime import date
from pathlib import Path

import duckdb
import httpx

SIH_BASE = "https://sih.conagua.gob.mx/basedatos/Presas"
WAREHOUSE = Path("warehouse.duckdb")
LATEST_DIR = Path("data/raw/latest")
CONCURRENCY = 8
TIMEOUT_S = 60.0
RETRIES = 3
TAIL_LINES = 60  # ~2 meses de margen vs el watermark


def get_dam_keys() -> list[str]:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    rows = con.execute(
        "SELECT dam_key FROM presas_catalogo ORDER BY numero"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


async def fetch_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    dam_key: str,
    out_dir: Path,
) -> tuple[str, bool, str]:
    url = f"{SIH_BASE}/{dam_key}.csv"
    out_path = out_dir / f"{dam_key}.csv"

    async with sem:
        for attempt in range(1, RETRIES + 1):
            try:
                r = await client.get(url)
                r.raise_for_status()
                # tail-trim: 8 líneas de header + últimas TAIL_LINES de datos
                lines = r.text.splitlines()
                if len(lines) > 8 + TAIL_LINES:
                    trimmed = lines[:8] + lines[-TAIL_LINES:]
                    out_path.write_text("\n".join(trimmed))
                    size_note = f"{len(lines)} → {len(trimmed)} líneas"
                else:
                    out_path.write_text(r.text)
                    size_note = f"{len(lines)} líneas (sin trim)"
                return (dam_key, True, size_note)
            except Exception as e:
                if attempt == RETRIES:
                    return (dam_key, False, f"{type(e).__name__}: {e}")
                await asyncio.sleep(2 ** attempt)
    return (dam_key, False, "unreachable")


async def main() -> None:
    keys = get_dam_keys()
    print(f"Catálogo: {len(keys)} presas")

    # limpia el directorio (siempre fresh)
    if LATEST_DIR.exists():
        shutil.rmtree(LATEST_DIR)
    LATEST_DIR.mkdir(parents=True)
    print(f"Destino: {LATEST_DIR} (sobrescrito)")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(timeout=TIMEOUT_S, follow_redirects=True) as client:
        tasks = [fetch_one(client, sem, k, LATEST_DIR) for k in keys]
        results = []
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            res = await coro
            results.append(res)
            if i % 30 == 0 or i == len(keys):
                ok_so_far = sum(1 for _, s, _ in results if s)
                print(f"  [{i:3}/{len(keys)}] ok={ok_so_far}")

    ok = sum(1 for _, s, _ in results if s)
    fail = [(k, m) for k, s, m in results if not s]
    print(f"\n✓ Exitosos: {ok}")
    print(f"✗ Fallidos: {len(fail)}")
    for k, m in fail:
        print(f"  {k}: {m}")

    # marca timestamp del último fetch (útil para alertas)
    (LATEST_DIR / ".fetched_at").write_text(date.today().isoformat())


if __name__ == "__main__":
    asyncio.run(main())