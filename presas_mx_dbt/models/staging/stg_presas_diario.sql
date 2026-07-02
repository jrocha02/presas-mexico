{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'presas_diario') }}
),

cleaned as (
    select
        dam_key,
        fecha,
        precipitacion_mm,
        evaporacion_mm,
        obra_toma_m3s,
        vertedor_m3s,
        derrame_m3s,
        volumen_alm_mm3,
        ingested_at
    from source
    where fecha >= date '1940-01-01'      -- filtra fechas centinela
      and fecha <= current_date            -- filtra futuros (data quality)
      and volumen_alm_mm3 >= 0             -- volúmenes negativos = error
)

select * from cleaned