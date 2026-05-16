-- Fails when sugars_100g is negative.
select
    barcode,
    sugars_100g
from {{ source('silver', 'silver_openfood_products') }}
where sugars_100g is not null
  and sugars_100g < 0
