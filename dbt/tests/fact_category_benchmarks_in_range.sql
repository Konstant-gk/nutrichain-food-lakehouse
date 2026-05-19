-- Fails when any category benchmark is outside per-100g plausibility bounds.
select
    product_id,
    category_avg_kcal_100g,
    category_avg_sugar_100g,
    category_avg_fat_100g,
    category_avg_salt_100g,
    category_avg_protein_100g
from {{ ref('fact_product_nutrition') }}
where
    (category_avg_kcal_100g is not null and (category_avg_kcal_100g < 0 or category_avg_kcal_100g > 900))
    or (category_avg_sugar_100g is not null and (category_avg_sugar_100g < 0 or category_avg_sugar_100g > 100))
    or (category_avg_fat_100g is not null and (category_avg_fat_100g < 0 or category_avg_fat_100g > 100))
    or (category_avg_salt_100g is not null and (category_avg_salt_100g < 0 or category_avg_salt_100g > 100))
    or (category_avg_protein_100g is not null and (category_avg_protein_100g < 0 or category_avg_protein_100g > 100))
