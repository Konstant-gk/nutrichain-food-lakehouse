-- Fails when more than 50% of Silver rows lack energy_kcal_per_100g (tune for your SLA).
with stats as (
    select
        sum(case when energy_kcal_per_100g is null then 1 else 0 end)
        / count(*) as null_pct
    from {{ source('silver', 'silver_openfood_products') }}
)
select null_pct
from stats
where null_pct > 0.50
