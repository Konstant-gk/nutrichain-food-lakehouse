-- Fails only when more than 5% of barcodes are not standard 8–14 digit EAN (OFF allows exceptions).
with stats as (
    select
        sum(
            case
                when barcode is null
                    or not regexp_like(trim(barcode), '^[0-9]{8,14}$')
                then 1
                else 0
            end
        ) / count(*) as invalid_ean_pct
    from {{ source('silver', 'silver_openfood_products') }}
)
select invalid_ean_pct
from stats
where invalid_ean_pct > 0.05
