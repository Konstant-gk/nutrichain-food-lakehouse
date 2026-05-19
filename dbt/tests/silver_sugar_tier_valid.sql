-- Fails when sugar_tier is not one of the EU tier labels produced in Silver
select
    barcode,
    sugar_tier
from {{ source('silver', 'silver_openfood_products') }}
where sugar_tier is not null
  and sugar_tier not in ('Low', 'Medium', 'High', 'Unknown')
