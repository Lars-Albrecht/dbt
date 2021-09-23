WITH excerpt AS(
SELECT DISTINCT
  DATETIME(v.value.created_at) AS created_at,
  v.value.sku,  
  v.value.price, 
  DATETIME(v.value.updated_at) AS updated_at
FROM `leslunes-raw.shopify_fr.products` 
LEFT JOIN UNNEST(variants) v
WHERE v.value.sku != ""
)

, final AS(

SELECT DISTINCT
  sku,
  price AS new_price,
  CASE WHEN lead(price) OVER(PARTITION BY sku ORDER BY updated_at DESC) IS NULL
    THEN created_at ELSE updated_at END AS price_valid_from,
  lead(price) OVER(PARTITION BY sku ORDER BY updated_at DESC) AS old_price
FROM excerpt 
)

SELECT * 
FROM final 
WHERE  new_price != old_price OR old_price IS NULL
ORDER BY DATETIME(price_valid_from) ASC, sku