
{{ config(
    materialized='incremental',
    unique_key=['dam_key', 'fecha'],
    on_schema_change='append_new_columns'
) }}

with src as (
    select * from {{ ref('stg_presas_diario') }}
    {% if is_incremental() %}
      where fecha >= (select max(fecha) - interval 7 day from {{ this }})
    {% endif %}
),

dim as (
    select dam_key, state, namo_oficial_mm3, name_oficial_mm3
    from {{ ref('dim_presa') }}
),

joined as (
    select
        s.dam_key,
        s.fecha,
        d.state,
        s.volumen_alm_mm3,
        d.namo_oficial_mm3,
        d.name_oficial_mm3,
        case
            when d.namo_oficial_mm3 > 0
            then round(100.0 * s.volumen_alm_mm3 / d.namo_oficial_mm3, 2)
        end as pct_llenado,
        case
            when d.name_oficial_mm3 > 0
            then round(100.0 * s.volumen_alm_mm3 / d.name_oficial_mm3, 2)
        end as pct_llenado_name,
        s.precipitacion_mm,
        s.evaporacion_mm,
        s.obra_toma_m3s,
        s.vertedor_m3s,
        s.derrame_m3s,
        lag(s.volumen_alm_mm3) over (
            partition by s.dam_key order by s.fecha
        ) as volumen_prev_mm3,
        s.volumen_alm_mm3 - lag(s.volumen_alm_mm3) over (
            partition by s.dam_key order by s.fecha
        ) as delta_volumen_mm3
    from src s
    left join dim d using (dam_key)
)

select * from joined