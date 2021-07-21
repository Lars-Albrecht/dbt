{{ config(materialized='table') }}

SELECT 
  product_name,	
  sku,	
  colour,	
  size,	
  fabric,
  gtin_ean,	
  hs_code	,
  product,
  adapted_name,
  Active_vs__Not_Active AS active_vs_not_active,	
  Last_FOB____ AS last_fob_euro,
  Last_FOB__ AS last_fob_dollars,	
  last_fob_price_date		,
  name_with_size	,
  category	
FROM `leslunes-raw.products.masterlist` 
WHERE product_name IS NOT NULL