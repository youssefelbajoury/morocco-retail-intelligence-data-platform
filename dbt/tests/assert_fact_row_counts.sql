select
    'fact_ventes' as fact_name,
    (select count(*) from {{ ref('int_sales') }}) as expected_rows,
    (select count(*) from {{ ref('fact_ventes') }}) as actual_rows
where (select count(*) from {{ ref('int_sales') }})
    <> (select count(*) from {{ ref('fact_ventes') }})
union all
select
    'fact_achats' as fact_name,
    (select count(*) from {{ ref('int_purchases') }}) as expected_rows,
    (select count(*) from {{ ref('fact_achats') }}) as actual_rows
where (select count(*) from {{ ref('int_purchases') }})
    <> (select count(*) from {{ ref('fact_achats') }})
union all
select
    'fact_stock' as fact_name,
    (select count(*) from {{ ref('int_inventory') }}) as expected_rows,
    (select count(*) from {{ ref('fact_stock') }}) as actual_rows
where (select count(*) from {{ ref('int_inventory') }})
    <> (select count(*) from {{ ref('fact_stock') }})
