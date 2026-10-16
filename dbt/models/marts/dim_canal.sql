select
    row_number() over (order by channel_id) as canal_sk,
    channel_id,
    channel_name,
    channel_type,
    demand_weight
from {{ ref('stg_channels') }}
