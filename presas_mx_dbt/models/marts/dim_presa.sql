{{ config(materialized='table') }}

with cat as (
    select * from {{ ref('stg_catalogo') }}
),

hist_stats as (
    select
        dam_key,
        min(fecha) as first_record_date,
        max(fecha) as last_record_date,
        count(*) as total_records,
        max(volumen_alm_mm3) as max_volumen_historico_mm3,
        quantile_cont(volumen_alm_mm3, 0.99) as volumen_p99_mm3
    from {{ ref('stg_presas_diario') }}
    where volumen_alm_mm3 is not null
    group by dam_key
)

select
    cat.*,
    hist.first_record_date,
    hist.last_record_date,
    hist.total_records,
    hist.max_volumen_historico_mm3,
    hist.volumen_p99_mm3 as namo_estimado_mm3,
    -- diferencia entre P99 estimado y NAMO oficial (sanity check)
    case
        when cat.namo_oficial_mm3 > 0
        then round(
            (hist.volumen_p99_mm3 - cat.namo_oficial_mm3) / cat.namo_oficial_mm3 * 100,
            2
        )
    end as namo_p99_vs_oficial_pct
from cat
left join hist_stats hist using (dam_key)