-- Fails when Gold fact energy_kcal_per_100g is outside 0–900 kcal per 100g.
select
    product_key,
    energy_kcal_per_100g
from {{ ref('fact_product_nutrition') }}
where energy_kcal_per_100g is not null
  and (
    energy_kcal_per_100g < 0
    or energy_kcal_per_100g > 900
  )
