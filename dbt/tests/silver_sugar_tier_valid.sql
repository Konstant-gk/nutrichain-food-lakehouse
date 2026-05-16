-- Fails when sugar_tier is not one of the EU tier labels produced in Silver.
select
    barcode,
    sugar_tier
from {{ source('silver', 'silver_openfood_products') }}
where sugar_tier is not null
  and lower(sugar_tier) not in ('low', 'medium', 'high', 'unknown')
