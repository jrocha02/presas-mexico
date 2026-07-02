{{ config(materialized='view') }}

with ranked as (
    select
        f.*,
        d.dam_name_clean,
        d.latitude,
        d.longitude,
        d.municipality,
        row_number() over (partition by dam_key order by fecha desc) as rn
    from {{ ref('fct_presa_diario') }} f
    left join {{ ref('dim_presa') }} d using (dam_key)
)

select * exclude (rn)
from ranked
where rn = 1