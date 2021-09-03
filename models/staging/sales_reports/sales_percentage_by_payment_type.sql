SELECT
  source AS shop,
  payment_type,
{% set col_dict = dbt_utils.get_query_results_as_dict("
WITH days AS (
SELECT * FROM UNNEST(GENERATE_DATE_ARRAY((SELECT min(date(created_at)) FROM `leslunes-prep.dbt_orders.orders_combined`), current_date()+31, INTERVAL 1 month)) AS day
)
select DISTINCT(SUBSTR(SAFE_CAST(day AS STRING),1,7)) as day from days
") %}
{%- for col in col_dict.day %}
SUM(CASE
    WHEN date = '{{col}}' THEN CAST(ROUND(payments_count/all_payments_count,4) AS NUMERIC)
END
    ) AS `_{{col[0:4]}}_{{col[5:7]}}`
{%-if not loop.last -%}
,
{%- endif -%}
{%- endfor %} 

FROM (
  SELECT
    SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS date,
    source,
    COUNT(*) AS all_payments_count
  FROM
    `leslunes-prep.dbt_orders.orders_combined`
  WHERE
    source_name='web' AND (gateway IS NOT NULL OR payment_gateway_names IS NOT NULL) AND gateway!='gift_card'
  GROUP BY
    date,
    source) AS all_payments
LEFT JOIN (
  SELECT
    date AS date2,
    source AS source2,
    payment_type,
    COUNT(*) AS payments_count
  FROM (
    SELECT
      SUBSTR(SAFE_CAST(created_at AS STRING),1,7) AS date,
      id,
      source,
      CASE
        WHEN UPPER(gateway) LIKE '%KLARNA%' THEN 'Klarna'
        WHEN UPPER(gateway) LIKE '%PAYPAL%' THEN 'Paypal'
        WHEN UPPER(gateway) LIKE '%SHOPIFY%' THEN 'Shopify'
        WHEN UPPER(gateway) LIKE '%COD%' THEN 'COD'
      #gateway sometimes shows 'manual', but had another payment method like klarna, paypal or shopify
        WHEN UPPER(payment_gateway_names) LIKE '%KLARNA%' THEN 'Klarna'
        WHEN UPPER(payment_gateway_names) LIKE '%PAYPAL%' THEN 'Paypal'
        WHEN UPPER(payment_gateway_names) LIKE '%SHOPIFY%' THEN 'Shopify'
        WHEN UPPER(payment_gateway_names) LIKE '%COD%' THEN 'COD'
      ELSE
      'Other'
    END
      AS payment_type
    FROM
      `leslunes-prep.dbt_orders.orders_combined` #`les-lunes-data-269915.import_combined.unique_orders`
    WHERE
      source_name='web' AND (gateway IS NOT NULL OR payment_gateway_names IS NOT NULL) AND gateway!='gift_card') AS grouped_payments
  GROUP BY
    date2,
    source2,
    payment_type) AS grouped_payments
ON
  all_payments.date=grouped_payments.date2
  AND all_payments.source=grouped_payments.source2
GROUP BY
  source,
  payment_type
ORDER BY
  source,
  payment_type DESC