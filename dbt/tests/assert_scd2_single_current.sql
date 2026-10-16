select
    product_id,
    count(*) as current_versions
from {{ ref('dim_produit') }}
where is_current
group by 1
having count(*) <> 1
