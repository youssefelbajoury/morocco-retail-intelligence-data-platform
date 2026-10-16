select
    row_number() over (order by region_id) as region_sk,
    region_id,
    region_name,
    demand_weight
from {{ ref('stg_regions') }}
