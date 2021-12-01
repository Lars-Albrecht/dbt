{{ config(materialized='table') }}

SELECT 
  product_name,	
  sku,	
  colour,	
  size,	
  fabric,
  gtin_ean,	
  hs_code,
  product,
  adapted_name,
  active_vs_not_active,	
  CAST(last_fob_euro AS FLOAT64) AS last_fob_euro,
  CAST(last_fob_dollar AS FLOAT64) AS last_fob_dollar,	
  last_fob_price_date,
  name_with_size,
  category	
FROM `leslunes-raw.products.masterlist` 
WHERE product_name IS NOT NULL
