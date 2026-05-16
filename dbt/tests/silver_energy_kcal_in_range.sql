-- Fails when energy_kcal_per_100g is outside a plausible per-100g band (0–900 kcal).
select
    barcode,
    energy_kcal_per_100g
from {{ source('silver', 'silver_openfood_products') }}
where energy_kcal_per_100g is not null
  and (
    energy_kcal_per_100g < 0
    or energy_kcal_per_100g > 900
  )
