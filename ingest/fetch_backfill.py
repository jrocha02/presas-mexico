"""
Descarga concurrente de los 210 CSVs del SIH CONAGUA.
Guarda en data/raw/YYYY-MM-DD/{dam_key}.csv

Uso:
    uv run python -m ingest.fetch
"""
from __future__ import annotations

import asyncio
from datetime import date
from pathlib import Path

import duckdb
import httpx

SIH_BASE = "https://sih.conagua.gob.mx/basedatos/Presas"
WAREHOUSE = Path("warehouse.duckdb")
RAW_DIR = Path("data/raw")
CONCURRENCY = 8           # paralelo razonable, no tumba al server
TIMEOUT_S = 60.0
RETRIES = 3


def get_dam_keys() -> list[str]:
    """Lee las 210 claves desde el seed cargado por dbt."""
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    # dbt-duckdb pone los seeds en main por default
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
                out_path.write_bytes(r.content)
                return (dam_key, True, f"{len(r.content):,} bytes")
            except Exception as e:
                if attempt == RETRIES:
                    return (dam_key, False, f"{type(e).__name__}: {e}")
                await asyncio.sleep(2 ** attempt)  # backoff exponencial
    return (dam_key, False, "unreachable")


async def main() -> None:
    keys = get_dam_keys()
    print(f"Catálogo: {len(keys)} presas")
    out_dir = RAW_DIR / f"backfill_{date.today().isoformat()}"

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Destino: {out_dir}")

    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(timeout=TIMEOUT_S, follow_redirects=True) as client:
        tasks = [fetch_one(client, sem, k, out_dir) for k in keys]
        results = []
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            res = await coro
            results.append(res)
            status = "✓" if res[1] else "✗"
            print(f"  [{i:3}/{len(keys)}] {status} {res[0]:12} {res[2]}")

    ok = sum(1 for _, success, _ in results if success)
    fail = len(results) - ok
    print(f"\n✓ Exitosos: {ok}")
    print(f"✗ Fallidos: {fail}")

    if fail:
        print("\nClaves con error:")
        for key, success, msg in results:
            if not success:
                print(f"  {key}: {msg}")
        # no fallamos hard — DuckDB cargará lo que sí bajó


if __name__ == "__main__":
    asyncio.run(main())