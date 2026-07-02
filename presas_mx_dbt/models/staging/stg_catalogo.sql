{{ config(materialized='view') }}

select
    numero, dam_key, state_code,
    dam_name, dam_name_clean, nombre_comun,
    state, municipality,
    latitude, longitude, altitude_m,
    basin_id, basin_name,
    hydro_region_id, hydro_region_name,
    namo_oficial_mm3, name_oficial_mm3,
    namo_elev_msnm, name_elev_msnm,
    altura_cortina_m, elev_corona_msnm,
    inicio_operacion, corriente, uso, tipo_vertedor
from {{ ref('presas_catalogo') }}