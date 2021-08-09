{{ config(materialized='table') }}

SELECT 
  PARSE_DATE("%Y-%m-%d",date) AS date	,
  SAFE_CAST(link_clicks AS INT64) AS link_clicks,
  SAFE_CAST(orders_fb AS INT64) AS orders_fb ,	
  SAFE_CAST(cac AS FLOAT64) AS CPO,	
  SAFE_CAST(payout_fb AS FLOAT64) AS payout_fb,
  SAFE_CAST(code_redemptions AS INT64) AS code_redemptions,
   SAFE_CAST(payout_code_redemptions AS FLOAT64) AS payout_code_redemptions
FROM `leslunes-raw.gsheets.fb_costs` 