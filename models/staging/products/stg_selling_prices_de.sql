WITH excerpt AS(
SELECT DISTINCT
  id AS product_id,
  v.value.id AS variant_id,
  DATETIME(v.value.created_at) AS created_at,
  v.value.sku,  
  v.value.price, 
  DATETIME(v.value.updated_at) AS updated_at
FROM `leslunes-raw.shopify_de.products` 
LEFT JOIN UNNEST(variants) v
WHERE v.value.sku != ""
ORDER BY sku ASC, updated_at ASC
)

, final AS(
  SELECT DISTINCT
    product_id,
    variant_id,
    sku,
    price AS new_price,
    CASE WHEN lead(price) OVER(PARTITION BY sku, product_id,variant_id ORDER BY updated_at DESC) IS NULL
      THEN created_at 
      ELSE updated_at 
    END AS price_valid_from,
    lead(price) OVER(PARTITION BY sku, product_id,variant_id ORDER BY updated_at DESC) AS old_price
  FROM excerpt
  ORDER BY price_valid_from ASC
)

SELECT
    product_id,
    variant_id,
    sku,
    new_price, 
    old_price, 
    price_valid_from,
    CASE WHEN LEAD(price_valid_from) OVER(PARTITION BY sku, product_id,variant_id ORDER BY price_valid_from ASC) IS NULL 
    THEN CURRENT_DATETIME() 
    ELSE LEAD(price_valid_from) OVER(PARTITION BY sku, product_id,variant_id ORDER BY price_valid_from ASC)
    END AS price_valid_till
FROM final 
WHERE  new_price != old_price OR old_price IS NULL
ORDER BY DATETIME(price_valid_from) ASC, sku