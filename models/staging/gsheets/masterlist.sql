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
  # error in retrieving exchange rate
  #SAFE_CAST(last_fob_euro AS FLOAT64) AS last_fob_euro,
  NULL AS last_fob_euro,
  SAFE_CAST(last_fob_dollar AS FLOAT64) AS last_fob_dollar,	
  last_fob_price_date,
  name_with_size,
  category,
  selling_status,
  SAFE_CAST(last_fob AS FLOAT64) AS last_fob,
  launch,
  sub_category,
  SAFE_CAST(weight AS float64) AS weight,
  product_color,
  SAFE_CAST(price_de AS float64) AS price_de,
  SAFE_CAST(price_fr AS float64) AS price_fr,
  SAFE_CAST(price_pl AS float64) AS price_pl,
  SAFE_CAST(fob_import AS float64) AS fob_import,
  SAFE_CAST(updated_xentral_sellable_stock AS int64) AS updated_xentral_sellable_stock, 
  gojungo_sku
FROM `leslunes-raw.products.masterlist` 
WHERE product_name IS NOT NULL
