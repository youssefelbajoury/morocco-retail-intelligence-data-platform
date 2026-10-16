with ordered as (
    select
        product_id,
        valid_from,
        valid_to,
        is_current,
        lag(valid_to) over (
            partition by product_id order by valid_from
        ) as previous_valid_to,
        lead(is_current) over (
            partition by product_id order by valid_from
        ) as later_is_current
    from {{ ref('dim_produit') }}
)
select
    product_id,
    valid_from,
    valid_to,
    previous_valid_to,
    later_is_current
from ordered
where (valid_to is null and later_is_current is not null)
    or (previous_valid_to is not null and valid_from <> previous_valid_to)
