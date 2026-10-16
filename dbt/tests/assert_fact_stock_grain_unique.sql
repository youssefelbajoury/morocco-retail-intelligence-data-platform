select
    date_sk,
    magasin_sk,
    produit_sk,
    count(*) as rows_at_grain
from {{ ref('fact_stock') }}
group by 1, 2, 3
having count(*) > 1
