{{ config(materialized='table') }}
SELECT
  year,
  month,
  dhl,
  collisimo,
  brt,
  ups,
  gls,
  zenfulfillment,
  fiege,
  bbpack,
  SAFE_CAST(special AS NUMERIC) AS special,
  SAFE_CAST(total AS NUMERIC) AS total,
  SAFE_CAST(total_orders AS NUMERIC) AS total_orders,
  SAFE_CAST(total_articles AS NUMERIC) AS total_articles,
  SAFE_CAST(piece_per_order AS NUMERIC) AS piece_per_order,
  SAFE_CAST(price_per_piece AS NUMERIC) AS price_per_order,
  SAFE_CAST(price_per_article AS NUMERIC) AS price_per_article,
  SAFE_CAST(customer_cards AS NUMERIC) AS customer_cards
FROM
  `leslunes-raw.gsheets.fulfillment_costs`
WHERE
  YEAR IS NOT NULL