select
    row_number() over (order by s.store_id) as magasin_sk,
    s.store_id,
    s.store_name,
    s.city,
    s.region_id,
    r.region_sk,
    s.store_format,
    s.size_sqm,
    s.open_date,
    s.is_active
from {{ ref('stg_stores') }} s
join {{ ref('dim_region') }} r on r.region_id = s.region_id
